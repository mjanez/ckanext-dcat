import json

from rdflib import URIRef, BNode, Literal
from .base import RDFProfile, CleanedURIRef, URIRefOrLiteral
from .base import (
    RDF,
    XSD,
    DCAT,
    DCT,
    VCARD,
    FOAF,
    SCHEMA,
    SKOS,
    LOCN,
)


class EuropeanDCATAPSchemingProfile(RDFProfile):
    """
    This is a compatibilty profile meant to add support for ckanext-scheming to the existing
    `euro_dcat_ap` and `euro_dcat_ap_2` profiles.
    It does not add or remove any properties from these profiles, it just transforms the
    resulting dataset_dict so it is compatible with a ckanext-scheming schema
    TODO: summarize changes and link to docs
    """

    def parse_dataset(self, dataset_dict, dataset_ref):

        if not self._dataset_schema:
            # Not using scheming
            return dataset_dict

        # Move extras to root

        extras_to_remove = []
        extras = dataset_dict.get("extras", [])
        for extra in extras:
            if self._schema_field(extra["key"]):
                # This is a field defined in the dataset schema
                dataset_dict[extra["key"]] = extra["value"]
                extras_to_remove.append(extra["key"])

        dataset_dict["extras"] = [e for e in extras if e["key"] not in extras_to_remove]

        # Parse lists
        def _parse_list_value(data_dict, field_name):
            schema_field = self._schema_field(
                field_name
            ) or self._schema_resource_field(field_name)

            if schema_field and "scheming_multiple_text" in schema_field.get(
                "validators", []
            ):
                if isinstance(data_dict[field_name], str):
                    try:
                        data_dict[field_name] = json.loads(data_dict[field_name])
                    except ValueError:
                        pass

        for field_name in dataset_dict.keys():
            _parse_list_value(dataset_dict, field_name)

        for resource_dict in dataset_dict.get("resources", []):
            for field_name in resource_dict.keys():
                _parse_list_value(resource_dict, field_name)

        # Repeating subfields
        new_fields_mapping = {
            "temporal_coverage": "temporal"
        }
        for schema_field in self._dataset_schema["dataset_fields"]:
            if "repeating_subfields" in schema_field:
                # Check if existing extras need to be migrated
                field_name = schema_field["field_name"]
                new_extras = []
                new_dict = {}
                check_name = new_fields_mapping.get(field_name, field_name)
                for extra in dataset_dict.get("extras", []):
                    if extra["key"].startswith(f"{check_name}_"):
                        subfield = extra["key"][extra["key"].index("_") + 1 :]
                        if subfield in [
                            f["field_name"] for f in schema_field["repeating_subfields"]
                        ]:
                            new_dict[subfield] = extra["value"]
                        else:
                            new_extras.append(extra)
                    else:
                        new_extras.append(extra)
                if new_dict:
                    dataset_dict[field_name] = [new_dict]
                    dataset_dict["extras"] = new_extras

        # Repeating subfields: resources
        for schema_field in self._dataset_schema["resource_fields"]:
            if "repeating_subfields" in schema_field:
                # Check if value needs to be load from JSON
                field_name = schema_field["field_name"]
                for resource_dict in dataset_dict.get("resources", []):
                    if resource_dict.get(field_name) and isinstance(
                        resource_dict[field_name], str
                    ):
                        try:
                            # TODO: load only subfields in schema?
                            resource_dict[field_name] = json.loads(
                                resource_dict[field_name]
                            )
                        except ValueError:
                            pass

        return dataset_dict

    def graph_from_dataset(self, dataset_dict, dataset_ref):

        contact = dataset_dict.get("contact")
        if isinstance(contact, list) and len(contact):
            for item in contact:
                contact_uri = item.get("uri")
                if contact_uri:
                    contact_details = CleanedURIRef(contact_uri)
                else:
                    contact_details = BNode()

                self.g.add((contact_details, RDF.type, VCARD.Organization))
                self.g.add((dataset_ref, DCAT.contactPoint, contact_details))

                self._add_triple_from_dict(item, contact_details, VCARD.fn, "name")
                # Add mail address as URIRef, and ensure it has a mailto: prefix
                self._add_triple_from_dict(
                    item,
                    contact_details,
                    VCARD.hasEmail,
                    "email",
                    _type=URIRef,
                    value_modifier=self._add_mailto,
                )

        publisher = dataset_dict.get("publisher")
        if isinstance(publisher, list) and len(publisher):
            publisher = publisher[0]
            publisher_uri = publisher.get("uri")
            if publisher_uri:
                publisher_ref = CleanedURIRef(publisher_uri)
            else:
                publisher_ref = BNode()

            self.g.add((publisher_ref, RDF.type, FOAF.Organization))
            self.g.add((dataset_ref, DCT.publisher, publisher_ref))

            self._add_triple_from_dict(publisher, publisher_ref, FOAF.name, "name")
            self._add_triple_from_dict(
                publisher, publisher_ref, FOAF.homepage, "url", URIRef
            )
            self._add_triple_from_dict(
                publisher, publisher_ref, DCT.type, "type", URIRefOrLiteral
            )
            self._add_triple_from_dict(
                publisher,
                publisher_ref,
                VCARD.hasEmail,
                "email",
                _type=URIRef,
                value_modifier=self._add_mailto,
            )

        temporal = dataset_dict.get("temporal_coverage")
        if isinstance(temporal, list) and len(temporal):
            for item in temporal:
                temporal_ref = BNode()
                self.g.add((temporal_ref, RDF.type, DCT.PeriodOfTime))
                if item.get("start"):
                    self._add_date_triple(temporal_ref, SCHEMA.startDate, item["start"])
                if item.get("end"):
                    self._add_date_triple(temporal_ref, SCHEMA.endDate, item["end"])
                self.g.add((dataset_ref, DCT.temporal, temporal_ref))

        spatial = dataset_dict.get("spatial_coverage")
        if isinstance(spatial, list) and len(spatial):
            for item in spatial:
                if item.get("uri"):
                    spatial_ref = CleanedURIRef(item["uri"])
                else:
                    spatial_ref = BNode()
                self.g.add((spatial_ref, RDF.type, DCT.Location))
                self.g.add((dataset_ref, DCT.spatial, spatial_ref))

                if item.get("text"):
                    self.g.add((spatial_ref, SKOS.prefLabel, Literal(item["text"])))

                for field in [
                    ("geom", LOCN.geometry),
                    ("bbox", DCAT.bbox),
                    ("centroid", DCAT.centroid),
                ]:
                    if item.get(field[0]):
                        self._add_spatial_value_to_graph(
                            spatial_ref, field[1], item[field[0]]
                        )

        resources = dataset_dict.get("resources", [])
        for resource in resources:
            if resource.get("access_services"):
                if isinstance(resource["access_services"], str):
                    try:
                        resource["access_services"] = json.loads(
                            resource["access_services"]
                        )
                    except ValueError:
                        pass
