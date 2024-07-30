import json

from rdflib import URIRef, Literal

from .base import RDFProfile, URIRefOrLiteral, CleanedURIRef
from .base import (
    XSD,
    DCAT,
    DCT,
    DCATAP,
)

from .spain_dcat import SpanishDCATProfile

# Default values
from ckanext.dcat.profiles.default_config import  euro_dcat_ap_default_values, spain_dcat_default_values


#TODO: Spanish profile GeoDCAT-AP extension
class SpanishDCATAPProfile(SpanishDCATProfile):
    '''
    An RDF profile based on the DCAT-AP to improve NTI-RISP profile for data portals in Spain

    More information and specification:

    https://semiceu.github.io/DCAT-AP/releases/3.0.0/

    '''
    def parse_dataset(self, dataset_dict, dataset_ref):
        """
        Parses a CKAN dataset dictionary and generates an RDF graph.

        Args:
            dataset_dict (dict): The dictionary containing the dataset metadata.
            dataset_ref (URIRef): The URI of the dataset in the RDF graph.

        Returns:
            dict: The updated dataset dictionary with the RDF metadata.
        """
        # call super method
        super(SpanishDCATAPProfile, self).parse_dataset(dataset_dict, dataset_ref)

        # Basic fields
        for key, predicate in (
                ('access_rights', DCT.accessRights),
                ('availability', DCATAP.availability),
                ):
            value = self._object_value(dataset_ref, predicate)
            if value:
                dataset_dict[key] = value

        # Lists
        for key, predicate in (
            ('temporal_resolution', DCAT.temporalResolution),
            ('is_referenced_by', DCT.isReferencedBy),
            ('applicable_legislation', DCATAP.applicableLegislation),
            ('hvd_category', DCATAP.hvdCategory),
        ):
            values = self._object_value_list(dataset_ref, predicate)
            if values:
                dataset_dict['extras'].append({'key': key,
                                               'value': json.dumps(values)})

        # Spatial resolution in meters
        spatial_resolution_in_meters = self._object_value_int_list(
            dataset_ref, DCAT.spatialResolutionInMeters)
        if spatial_resolution_in_meters:
            dataset_dict['extras'].append({'key': 'spatial_resolution_in_meters',
                                           'value': json.dumps(spatial_resolution_in_meters)})

    def graph_from_dataset(self, dataset_dict, dataset_ref):
        """
        Generates an RDF graph from a dataset dictionary.

        Args:
            dataset_dict (dict): The dictionary containing the dataset metadata.
            dataset_ref (URIRef): The URI of the dataset in the RDF graph.

        Returns:
            None
        """
        
        # call super method
        super(SpanishDCATAPProfile, self).graph_from_dataset(dataset_dict, dataset_ref)

        # DCAT-AP Extension
        geodcatap_items = {
            'availability': (DCATAP.availability, spain_dcat_default_values['availability']),
        }

        self._add_dataset_triples_from_dict(dataset_dict, dataset_ref, geodcatap_items)    

        # Lists
        for key, predicate in (
            ('is_referenced_by', DCT.isReferencedBy),
            ('applicable_legislation', DCATAP.applicableLegislation),
            ('hvd_category', DCATAP.hvdCategory),
        ):
            values = self._object_value_list(dataset_ref, predicate)
            if values:
                dataset_dict['extras'].append({'key': key,
                                               'value': json.dumps(values)})
        # Spatial resolution in meters
        spatial_resolution_in_meters = self._get_dataset_value(dataset_dict, 'spatial_resolution_in_meters')
        if spatial_resolution_in_meters:
            try:
                self.g.add((dataset_ref, DCAT.spatialResolutionInMeters,
                            Literal(float(spatial_resolution_in_meters), datatype=XSD.decimal)))
            except (ValueError, TypeError):
                self.g.add((dataset_ref, DCAT.spatialResolutionInMeters, Literal(spatial_resolution_in_meters)))

        # Access Rights
        # DCAT-AP: http://publications.europa.eu/en/web/eu-vocabularies/at-dataset/-/resource/dataset/access-right
        if self._get_dataset_value(dataset_dict, 'access_rights') and 'authority/access-right' in self._get_dataset_value(dataset_dict, 'access_rights'):
                self.g.add((dataset_ref, DCT.accessRights, URIRef(self._get_dataset_value(dataset_dict, 'access_rights'))))
        else:
            self.g.add((dataset_ref, DCT.accessRights, URIRef(euro_dcat_ap_default_values['access_rights'])))

    def graph_from_catalog(self, catalog_dict, catalog_ref):
        """
        Adds the metadata of a CKAN catalog to the RDF graph.

        Args:
            catalog_dict (dict): A dictionary containing the metadata of the catalog.
            catalog_ref (URIRef): The URI of the catalog in the RDF graph.

        Returns:
            None
        """

        # call super method
        super(SpanishDCATAPProfile, self).graph_from_catalog(catalog_dict, catalog_ref)
