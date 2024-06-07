import json

from rdflib import term, URIRef, BNode, Literal
import ckantoolkit as toolkit

from ckan.lib.munge import munge_tag

from ckanext.dcat.utils import (
    resource_uri,
    DCAT_EXPOSE_SUBCATALOGS,
    DCAT_CLEAN_TAGS,
    publisher_uri_organization_fallback,
)
from .base import RDFProfile, URIRefOrLiteral, CleanedURIRef
from .base import (
    RDF,
    XSD,
    SKOS,
    RDFS,
    DCAT,
    DCT,
    ADMS,
    XSD,
    VCARD,
    FOAF,
    SCHEMA,
    SKOS,
    LOCN,
    GSP,
    OWL,
    SPDX,
    GEOJSON_IMT,
    CNT,
    namespaces,
    MD_INSPIRE_REGISTER,
    MD_FORMAT,
    MD_EU_LANGUAGES,
    DISTRIBUTION_LICENSE_FALLBACK_CONFIG,
)

# Default values
from ckanext.dcat.profiles.default_config import metadata_field_names, euro_dcat_ap_default_values, default_translated_fields

config = toolkit.config


class EuropeanDCATAPProfile(RDFProfile):
    '''
    An RDF profile based on the DCAT-AP for data portals in Europe

    Default values for some fields:
    
    ckanext-dcat/ckanext/dcat/.profiles.default_config.py

    More information and specification:

    https://joinup.ec.europa.eu/asset/dcat_application_profile

    '''

    def parse_dataset(self, dataset_dict, dataset_ref):

        dataset_dict['extras'] = []
        dataset_dict['resources'] = []

        # Translated fields
        for key, field in default_translated_fields.items():
            predicate = field['rdf_predicate']
            value = self._object_value(dataset_ref, predicate, True)
            if value:
                dataset_dict[field['field_name']] = value

        # Basic fields
        for key, predicate in (
                ('title', DCT.title),
                ('notes', DCT.description),
                ('url', DCAT.landingPage),
                ('version', OWL.versionInfo),
                ('encoding', CNT.characterEncoding)
                ):
            value = self._object_value(dataset_ref, predicate)
            if value:
                dataset_dict[key] = value

        if not dataset_dict.get('version'):
            # adms:version was supported on the first version of the DCAT-AP
            value = self._object_value(dataset_ref, ADMS.version)
            if value:
                dataset_dict['version'] = value

        # Tags
        # replace munge_tag to noop if there's no need to clean tags
        do_clean = toolkit.asbool(config.get(DCAT_CLEAN_TAGS, False))
        tags_val = [munge_tag(tag) if do_clean else tag for tag in self._keywords(dataset_ref)]
        tags = [{'name': tag} for tag in tags_val]
        dataset_dict['tags'] = tags

        # Extras

        #  Simple values
        for key, predicate in (
                ('issued', DCT.issued),
                ('modified', DCT.modified),
                ('identifier', DCT.identifier),
                ('version_notes', ADMS.versionNotes),
                ('frequency', DCT.accrualPeriodicity),
                ('provenance', DCT.provenance),
                ('dcat_type', DCT.type),
                ):
            value = self._object_value(dataset_ref, predicate)
            if value:
                dataset_dict['extras'].append({'key': key, 'value': value})

        #  Lists
        for key, predicate, in (
                ('language', DCT.language),
                ('theme', DCAT.theme),
                (metadata_field_names['spain_dcat']['theme'], DCAT.theme),
                ('alternate_identifer', ADMS.identifier),
                ('inspire_id', ADMS.identifier),
                ('conforms_to', DCT.conformsTo),
                ('metadata_profile', DCT.conformsTo),
                ('documentation', FOAF.page),
                ('related_resource', DCT.relation),
                ('has_version', DCT.hasVersion),
                ('is_version_of', DCT.isVersionOf),
                ('lineage_source', DCT.source),
                ('source', DCT.source),
                ('sample', ADMS.sample),
                ):
            values = self._object_value_list(dataset_ref, predicate)
            if values:
                dataset_dict['extras'].append({'key': key,
                                               'value': json.dumps(values)})

        # Contact details
        contact = self._contact_details(dataset_ref, DCAT.contactPoint)
        if not contact:
            # adms:contactPoint was supported on the first version of DCAT-AP
            contact = self._contact_details(dataset_ref, ADMS.contactPoint)

        if contact:
            for key in ('uri', 'name', 'email', 'url', 'role'):
                if contact.get(key):
                    dataset_dict['extras'].append(
                        {'key': 'contact_{0}'.format(key),
                         'value': contact.get(key)})

        # Publisher
        publisher = self._publisher(dataset_ref, DCT.publisher)
        for key in ('uri', 'name', 'email', 'url', 'type', 'identifier'):
            if publisher.get(key):
                dataset_dict['extras'].append(
                    {'key': 'publisher_{0}'.format(key),
                     'value': publisher.get(key)})

        # Temporal
        start, end = self._time_interval(dataset_ref, DCT.temporal)
        if start:
            dataset_dict['extras'].append(
                {'key': 'temporal_start', 'value': start})
        if end:
            dataset_dict['extras'].append(
                {'key': 'temporal_end', 'value': end})

        # Spatial
        spatial = self._spatial(dataset_ref, DCT.spatial)
        for key in ('uri', 'text', 'geom'):
            self._add_spatial_to_dict(dataset_dict, key, spatial)

        # Dataset URI (explicitly show the missing ones)
        dataset_uri = (str(dataset_ref)
                       if isinstance(dataset_ref, URIRef)
                       else '')
        dataset_dict['extras'].append({'key': 'uri', 'value': dataset_uri})

        # License
        if 'license_id' not in dataset_dict:
            dataset_dict['license_id'] = self._license(dataset_ref)

        # Source Catalog
        if toolkit.asbool(config.get(DCAT_EXPOSE_SUBCATALOGS, False)):
            catalog_src = self._get_source_catalog(dataset_ref)
            if catalog_src is not None:
                src_data = self._extract_catalog_dict(catalog_src)
                dataset_dict['extras'].extend(src_data)

        # Resources
        for distribution in self._distributions(dataset_ref):

            resource_dict = {}

            #  Simple values
            for key, predicate in (
                    ('name', DCT.title),
                    ('description', DCT.description),
                    ('access_url', DCAT.accessURL),
                    ('download_url', DCAT.downloadURL),
                    ('issued', DCT.issued),
                    ('modified', DCT.modified),
                    ('status', ADMS.status),
                    ('license', DCT.license),
                    ):
                value = self._object_value(distribution, predicate)
                if value:
                    resource_dict[key] = value

            resource_dict['url'] = (self._object_value(distribution,
                                                       DCAT.downloadURL) or
                                    self._object_value(distribution,
                                                       DCAT.accessURL))
            #  Lists
            for key, predicate in (
                    ('language', DCT.language),
                    ('documentation', FOAF.page),
                    ('conforms_to', DCT.conformsTo),
                    ('metadata_profile', DCT.conformsTo),
                    ):
                values = self._object_value_list(distribution, predicate)
                if values:
                    resource_dict[key] = json.dumps(values)

            # rights
            rights = self._access_rights(distribution, DCT.rights)
            if rights:
                resource_dict['rights'] = rights

            # Format and media type
            normalize_ckan_format = toolkit.asbool(config.get(
                'ckanext.dcat.normalize_ckan_format', True))
            imt, label = self._distribution_format(distribution,
                                                   normalize_ckan_format)

            if imt:
                resource_dict['mimetype'] = imt

            if label:
                resource_dict['format'] = label
            elif imt:
                resource_dict['format'] = imt

            # Size
            size = self._object_value_int(distribution, DCAT.byteSize)
            if size is not None:
                resource_dict['size'] = size

            # Checksum
            for checksum in self.g.objects(distribution, SPDX.checksum):
                algorithm = self._object_value(checksum, SPDX.algorithm)
                checksum_value = self._object_value(checksum, SPDX.checksumValue)
                if algorithm:
                    resource_dict['hash_algorithm'] = algorithm
                if checksum_value:
                    resource_dict['hash'] = checksum_value

            # Distribution URI (explicitly show the missing ones)
            resource_dict['uri'] = (str(distribution)
                                    if isinstance(distribution,
                                                  URIRef)
                                    else '')

            # Remember the (internal) distribution reference for referencing in
            # further profiles, e.g. for adding more properties
            resource_dict['distribution_ref'] = str(distribution)

            dataset_dict['resources'].append(resource_dict)

        if self.compatibility_mode:
            # Tweak the resulting dict to make it compatible with previous
            # versions of the ckanext-dcat parsers
            for extra in dataset_dict['extras']:
                if extra['key'] in ('issued', 'modified', 'publisher_name',
                                    'publisher_email',):

                    extra['key'] = 'dcat_' + extra['key']

                if extra['key'] == 'language':
                    extra['value'] = ','.join(
                        sorted(json.loads(extra['value'])))

        return dataset_dict

    def graph_from_dataset(self, dataset_dict, dataset_ref):

        g = self.g

        for prefix, namespace in namespaces.items():
            g.bind(prefix, namespace)

        g.add((dataset_ref, RDF.type, DCAT.Dataset))

        # Translated fields
        items = [(
            default_translated_fields[key]['field_name'],
            default_translated_fields[key]['rdf_predicate'],
            default_translated_fields[key]['fallbacks'],
            default_translated_fields[key]['_type']
            )
            for key in default_translated_fields
        ]
        self._add_triples_from_dict(dataset_dict, dataset_ref, items, multilang=True)

        # Basic fields
        items = [
            ('encoding', CNT.characterEncoding, None, Literal),
            ('url', DCAT.landingPage, None, URIRef),
            ('identifier', DCT.identifier, ['guid', 'id'], URIRefOrLiteral),
            ('version', OWL.versionInfo, ['dcat_version'], Literal),
            ('frequency', DCT.accrualPeriodicity, None, URIRefOrLiteral),
            ('dcat_type', DCT.type, None, URIRefOrLiteral),
            ('topic', DCAT.keyword, None, URIRefOrLiteral),
        ]
        self._add_triples_from_dict(dataset_dict, dataset_ref, items)

        # Access Rights
        # DCAT-AP: http://publications.europa.eu/en/web/eu-vocabularies/at-dataset/-/resource/dataset/access-right
        if self._get_dataset_value(dataset_dict, 'access_rights') and 'authority/access-right' in self._get_dataset_value(dataset_dict, 'access_rights'):
                g.add((dataset_ref, DCT.accessRights, URIRef(self._get_dataset_value(dataset_dict, 'access_rights'))))
        else:
            g.add((dataset_ref, DCT.accessRights, URIRef(euro_dcat_ap_default_values['access_rights'])))
            
        # Tags
        # Pre-process keywords inside INSPIRE MD Codelists and update dataset_dict
        dataset_tag_base = f"{dataset_ref.split('/dataset/')[0]}"
        tag_names = [tag['name'].replace(" ", "").lower() for tag in dataset_dict.get('tags', [])]

        # Search for matching keywords in MD_INSPIRE_REGISTER and update dataset_dict
        if tag_names:             
            self._search_values_codelist_add_to_graph(MD_INSPIRE_REGISTER, tag_names, dataset_dict, dataset_ref, dataset_tag_base, g, DCAT.keyword)

        # Dates
        items = [
            ('created', DCT.created, ['metadata_created'], Literal),
            ('issued', DCT.issued, ['metadata_created'], Literal),
            ('modified', DCT.modified, ['metadata_modified'], Literal),
        ]
        self._add_date_triples_from_dict(dataset_dict, dataset_ref, items)

        #  Lists
        items = [
            ('language', DCT.language, None, URIRefOrLiteral),
            ('theme', DCAT.theme, None, URIRef),
            (metadata_field_names['spain_dcat']['theme'], DCAT.theme, None, URIRef),
            ('conforms_to', DCT.conformsTo, None, URIRef),
            ('metadata_profile', DCT.conformsTo, None, URIRef),
            ('alternate_identifier', ADMS.identifier, None, URIRefOrLiteral),
            ('inspire_id', ADMS.identifier, None, URIRefOrLiteral),
            ('documentation', FOAF.page, None, URIRefOrLiteral),
            ('related_resource', DCT.relation, None, URIRefOrLiteral),
            ('has_version', DCT.hasVersion, None, URIRefOrLiteral),
            ('is_version_of', DCT.isVersionOf, None, URIRefOrLiteral),
            ('lineage_source', DCT.source, None, Literal),
            ('source', DCT.source, None, URIRefOrLiteral),
            ('sample', ADMS.sample, None, URIRefOrLiteral),
        ]
        self._add_list_triples_from_dict(dataset_dict, dataset_ref, items)

        # DCAT Themes (https://publications.europa.eu/resource/authority/data-theme)
        # Append the final result to the graph
        dcat_themes = self._themes(dataset_ref)
        for theme in dcat_themes:
            g.add((dataset_ref, DCAT.theme, URIRefOrLiteral(theme)))

        # Contact details
        if any([
            self._get_dataset_value(dataset_dict, 'contact_uri'),
            self._get_dataset_value(dataset_dict, 'contact_name'),
            self._get_dataset_value(dataset_dict, 'contact_email'),
            self._get_dataset_value(dataset_dict, 'contact_url'),
        ]):

            contact_uri = self._get_dataset_value(dataset_dict, 'contact_uri')
            if contact_uri:
                contact_details = CleanedURIRef(contact_uri)
            else:
                contact_details = BNode()

            g.add((contact_details, RDF.type, VCARD.Organization))
            g.add((dataset_ref, DCAT.contactPoint, contact_details))

            # Add name
            self._add_triple_from_dict(
                dataset_dict, contact_details,
                VCARD.fn, 'contact_name'
            )
            # Add mail address as URIRef, and ensure it has a mailto: prefix
            self._add_triple_from_dict(
                dataset_dict, contact_details,
                VCARD.hasEmail, 'contact_email',
                _type=URIRef, value_modifier=self._add_mailto
            )
            # Add contact URL
            self._add_triple_from_dict(
                dataset_dict, contact_details,
                VCARD.hasURL, 'contact_url',
                _type=URIRef)
            
            # Add contact role
            g.add((contact_details, VCARD.role, URIRef(euro_dcat_ap_default_values['contact_role'])))

        # Resource maintainer/contact 
        if any([
            self._get_dataset_value(dataset_dict, 'maintainer'),
            self._get_dataset_value(dataset_dict, 'maintainer_uri'),
            self._get_dataset_value(dataset_dict, 'maintainer_email'),
            self._get_dataset_value(dataset_dict, 'maintainer_url'),
        ]):
            maintainer_uri = self._get_dataset_value(dataset_dict, 'maintainer_uri')
            if maintainer_uri:
                maintainer_details = CleanedURIRef(maintainer_uri)
            else:
                maintainer_details = dataset_ref + '/maintainer'
                
            g.add((maintainer_details, RDF.type, VCARD.Individual))
            g.add((dataset_ref, DCAT.contactPoint, maintainer_details))

            ## Add name & mail
            self._add_triple_from_dict(
                dataset_dict, maintainer_details,
                VCARD.fn, 'maintainer'
            )
            self._add_triple_from_dict(
                dataset_dict, maintainer_details,
                VCARD.hasEmail, 'maintainer_email',
                _type=URIRef, value_modifier=self._add_mailto
            )

            # Add maintainer URL
            self._add_triple_from_dict(
                dataset_dict, maintainer_details,
                VCARD.hasURL, 'maintainer_url',
                _type=URIRef)

            # Add maintainer role
            g.add((maintainer_details, VCARD.role, URIRef(euro_dcat_ap_default_values['maintainer_role'])))

        # Resource author
        if any([
            self._get_dataset_value(dataset_dict, 'author'),
            self._get_dataset_value(dataset_dict, 'author_uri'),
            self._get_dataset_value(dataset_dict, 'author_email'),
            self._get_dataset_value(dataset_dict, 'author_url'),
        ]):
            author_uri = self._get_dataset_value(dataset_dict, 'author_uri')
            if author_uri:
                author_details = CleanedURIRef(author_uri)
            else:
                author_details = dataset_ref + '/author'
                
            g.add((author_details, RDF.type, VCARD.Organization))
            g.add((dataset_ref, DCT.creator, author_details))

            ## Add name & mail
            self._add_triple_from_dict(
                dataset_dict, author_details,
                VCARD.fn, 'author'
            )
            self._add_triple_from_dict(
                dataset_dict, author_details,
                VCARD.hasEmail, 'author_email',
                _type=URIRef, value_modifier=self._add_mailto
            )

            # Add author URL
            self._add_triple_from_dict(
                dataset_dict, author_details,
                VCARD.hasURL, 'author_url',
                _type=URIRef)

            # Add author role
            g.add((author_details, VCARD.role, URIRef(euro_dcat_ap_default_values['author_role'])))

        # Provenance: dataset dct:provenance dct:ProvenanceStatement
        provenance_details = dataset_ref + '/provenance'
        provenance_statement = self._get_dataset_value(dataset_dict, 'provenance')
        if provenance_statement:
            g.add((dataset_ref, DCT.provenance, provenance_details))
            g.add((provenance_details, RDF.type, DCT.ProvenanceStatement))
            
            if isinstance(provenance_statement, dict):
                self._add_multilang_triple(provenance_details, RDFS.label, provenance_statement)
            else:
                g.add((provenance_details, RDFS.label, Literal(provenance_statement)))

        # Publisher
        if any([
            self._get_dataset_value(dataset_dict, 'publisher_uri'),
            self._get_dataset_value(dataset_dict, 'publisher_name'),
            dataset_dict.get('organization'),
        ]):

            publisher_uri = self._get_dataset_value(dataset_dict, 'publisher_uri')
            publisher_uri_fallback = publisher_uri_organization_fallback(dataset_dict)
            publisher_name = self._get_dataset_value(dataset_dict, 'publisher_name')
            if publisher_uri:
                publisher_details = CleanedURIRef(publisher_uri)
            elif not publisher_name and publisher_uri_fallback:
                # neither URI nor name are available, use organization as fallback
                publisher_details = CleanedURIRef(publisher_uri_fallback)
            else:
                # No publisher_uri
                publisher_details = BNode()

            g.add((publisher_details, RDF.type, FOAF.Organization))
            g.add((dataset_ref, DCT.publisher, publisher_details))

            # In case no name and URI are available, again fall back to organization.
            # If no name but an URI is available, the name literal remains empty to
            # avoid mixing organization and dataset values.
            if not publisher_name and not publisher_uri and dataset_dict.get('organization'):
                publisher_name = dataset_dict['organization']['title']

            g.add((publisher_details, FOAF.name, Literal(publisher_name)))
            # TODO: It would make sense to fallback these to organization
            # fields but they are not in the default schema and the
            # `organization` object in the dataset_dict does not include
            # custom fields
            items = [
                ('publisher_email', FOAF.mbox, None, Literal),
                ('publisher_url', FOAF.homepage, None, URIRef),
                ('publisher_type', DCT.type, None, URIRefOrLiteral),
                ('publisher_identifier', DCT.identifier, None, URIRefOrLiteral)
            ]

            # Add publisher role
            g.add((publisher_details, VCARD.role, URIRef(euro_dcat_ap_default_values['publisher_role'])))

            self._add_triples_from_dict(dataset_dict, publisher_details, items)

        # TODO: Deprecated: https://semiceu.github.io/GeoDCAT-AP/drafts/latest/#deprecated-properties-for-period-of-time
        # Temporal
        '''        
        start = self._get_dataset_value(dataset_dict, 'temporal_start')
        end = self._get_dataset_value(dataset_dict, 'temporal_end')
        if start or end:
            temporal_extent = BNode()

            g.add((temporal_extent, RDF.type, DCT.PeriodOfTime))
            if start:
                self._add_date_triple(temporal_extent, SCHEMA.startDate, start)
            if end:
                self._add_date_triple(temporal_extent, SCHEMA.endDate, end)
            g.add((dataset_ref, DCT.temporal, temporal_extent))
        '''

        # Spatial
        spatial_text = self._get_dataset_value(dataset_dict, 'spatial_text')
        spatial_geom = self._get_dataset_value(dataset_dict, 'spatial')

        if spatial_text or spatial_geom:
            spatial_ref = self._get_or_create_spatial_ref(dataset_dict, dataset_ref)

            if spatial_text:
                g.add((spatial_ref, SKOS.prefLabel, Literal(spatial_text)))

            # Deprecated by dcat:bbox
            # if spatial_geom:
            #     self._add_spatial_value_to_graph(spatial_ref, LOCN.geometry, spatial_geom)

        # Coordinate Reference System
        if self._get_dataset_value(dataset_dict, 'reference_system'):
            crs_uri = self._get_dataset_value(dataset_dict, 'reference_system')
            crs_details = CleanedURIRef(crs_uri)
            g.add((crs_details, RDF.type, DCT.Standard))
            g.add((crs_details, DCT.type, CleanedURIRef(euro_dcat_ap_default_values['reference_system_type'])))
            g.add((dataset_ref, DCT.conformsTo, crs_details))

        # Use fallback license if set in config
        resource_license_fallback = None
        if toolkit.asbool(config.get(DISTRIBUTION_LICENSE_FALLBACK_CONFIG, False)):
            if 'license_id' in dataset_dict and isinstance(URIRefOrLiteral(dataset_dict['license_id']), URIRef):
                resource_license_fallback = dataset_dict['license_id']
            elif 'license_url' in dataset_dict and isinstance(URIRefOrLiteral(dataset_dict['license_url']), URIRef):
                resource_license_fallback = dataset_dict['license_url']

        # Resources
        for resource_dict in dataset_dict.get('resources', []):

            distribution = CleanedURIRef(resource_uri(resource_dict))

            g.add((dataset_ref, DCAT.distribution, distribution))

            g.add((distribution, RDF.type, DCAT.Distribution))

            #  Simple values
            items = [
                ('name', DCT.title, None, Literal),
                ('encoding', CNT.characterEncoding, None, Literal),
                ('description', DCT.description, None, Literal),
                ('status', ADMS.status, None, URIRefOrLiteral),
                ('license', DCT.license, None, URIRefOrLiteral),
                ('access_url', DCAT.accessURL, None, URIRef),
                ('download_url', DCAT.downloadURL, None, URIRef),
            ]

            self._add_triples_from_dict(resource_dict, distribution, items)

            #  Lists
            items = [
                ('documentation', FOAF.page, None, URIRefOrLiteral),
                ('language', DCT.language, None, URIRefOrLiteral),
                ('conforms_to', DCT.conformsTo, None, URIRef),
                ('metadata_profile', DCT.conformsTo, None, URIRef),
            ]
            self._add_list_triples_from_dict(resource_dict, distribution, items)

            # Set default license for distribution if needed and available
            if resource_license_fallback and not (distribution, DCT.license, None) in g:
                g.add((distribution, DCT.license, URIRefOrLiteral(resource_license_fallback)))

            # Format
            mimetype = resource_dict.get('mimetype')
            fmt = resource_dict.get('format').replace(" ", "")

            # IANA media types (either URI or Literal) should be mapped as mediaType.
            # In case format is available and mimetype is not set or identical to format,
            # check which type is appropriate.
            if mimetype:
                g.add((distribution, DCAT.mediaType, URIRefOrLiteral(mimetype)))
            elif fmt:
                mime_val = self._search_value_codelist(MD_FORMAT, fmt, "id", "media_type") or None
                if mime_val and mime_val != fmt:
                    g.add((distribution, DCAT.mediaType, URIRefOrLiteral(mime_val)))

            # Try to match format field
            fmt = self._search_value_codelist(MD_FORMAT, fmt, "label", "id") or fmt

            # Add format to graph
            if fmt:
                g.add((distribution, DCT['format'], URIRefOrLiteral(fmt)))

            # URL fallback and old behavior
            url = resource_dict.get('url')
            download_url = resource_dict.get('download_url')
            access_url = resource_dict.get('access_url')
            # Use url as fallback for access_url if access_url is not set and download_url is not equal
            if url and not access_url:
                if (not download_url) or (download_url and url != download_url):
                  self._add_triple_from_dict(resource_dict, distribution, DCAT.accessURL, 'url', _type=URIRef)

            # Dates
            items = [
                ('issued', DCT.issued, None, Literal),
                ('modified', DCT.modified, None, Literal),
            ]

            self._add_date_triples_from_dict(resource_dict, distribution, items)

            # DCAT-AP: http://publications.europa.eu/en/web/eu-vocabularies/at-dataset/-/resource/dataset/access-right
            access_rights = self._get_resource_value(resource_dict, 'rights')
            if access_rights and 'authority/access-right' in access_rights:
                access_rights_uri = URIRef(access_rights)
            else:
                access_rights_uri = URIRef(euro_dcat_ap_default_values['access_rights'])
            g.add((distribution, DCT.rights, access_rights_uri))

            # Numbers
            if resource_dict.get('size'):
                try:
                    g.add((distribution, DCAT.byteSize,
                           Literal(float(resource_dict['size']),
                                   datatype=XSD.decimal)))
                except (ValueError, TypeError):
                    g.add((distribution, DCAT.byteSize,
                           Literal(resource_dict['size'])))
            # Checksum
            if resource_dict.get('hash'):
                checksum = BNode()
                g.add((checksum, RDF.type, SPDX.Checksum))
                g.add((checksum, SPDX.checksumValue,
                       Literal(resource_dict['hash'],
                               datatype=XSD.hexBinary)))

                if resource_dict.get('hash_algorithm'):
                    g.add((checksum, SPDX.algorithm,
                           URIRefOrLiteral(resource_dict['hash_algorithm'])))

                g.add((distribution, SPDX.checksum, checksum))

    def graph_from_catalog(self, catalog_dict, catalog_ref):

        g = self.g

        for prefix, namespace in namespaces.items():
            g.bind(prefix, namespace)

        g.add((catalog_ref, RDF.type, DCAT.Catalog))
        
        # Basic fields
        license, publisher_identifier, access_rights, spatial_uri, language = [
            self._get_catalog_field(field_name='license_url'),
            self._get_catalog_field(field_name='publisher_identifier', fallback='publisher_uri'),
            self._get_catalog_field(field_name='access_rights'),
            self._get_catalog_field(field_name='spatial_uri'),
            self._search_value_codelist(MD_EU_LANGUAGES, config.get('ckan.locale_default'), "label","id") or euro_dcat_ap_default_values['language'],
            ]

        # Mandatory elements by NTI-RISP (datos.gob.es)
        items = [
            ('identifier', DCT.identifier, f'{config.get("ckan_url")}/catalog.rdf', Literal),
            ('title', DCT.title, config.get('ckan.site_title'), Literal),
            ('encoding', CNT.characterEncoding, 'UTF-8', Literal),
            ('description', DCT.description, config.get('ckan.site_description'), Literal),
            ('publisher_identifier', DCT.publisher, publisher_identifier, URIRef),
            ('language', DCT.language, language, URIRefOrLiteral),
            ('spatial_uri', DCT.spatial, spatial_uri, URIRefOrLiteral),
            ('theme_taxonomy', DCAT.themeTaxonomy, euro_dcat_ap_default_values['theme_taxonomy'], URIRef),
            ('homepage', FOAF.homepage, config.get('ckan_url'), URIRef),
            ('license', DCT.license, license, URIRef),
            ('conforms_to', DCT.conformsTo, euro_dcat_ap_default_values['conformance'], URIRef),
            ('access_rights', DCT.accessRights, access_rights, URIRefOrLiteral),
        ]
                 
        for item in items:
            key, predicate, fallback, _type = item
            value = catalog_dict.get(key, fallback) if catalog_dict else fallback
            if value:
                g.add((catalog_ref, predicate, _type(value)))

        # Dates
        modified = self._get_catalog_field(field_name='metadata_modified')
        issued = self._get_catalog_field(field_name='metadata_created', order='asc')
        if modified or issued or license:
            if modified:
                self._add_date_triple(catalog_ref, DCT.modified, modified)
            if issued:
                self._add_date_triple(catalog_ref, DCT.issued, issued)
