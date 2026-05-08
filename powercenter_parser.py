"""
powercenter_parser.py
Parser for Informatica PowerCenter XML.
Captures logical database names and connection information.
"""

import json
from lxml import etree
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any
import sys

# ----------------------------------------------------------------------
# Data model
# ----------------------------------------------------------------------
@dataclass
class Port:
    name: str
    data_type: str = ""
    precision: str = ""
    scale: str = ""
    porttype: str = ""
    default_value: str = ""
    description: str = ""
    expression: str = ""
    parent_transform_name: str = ""
    source_transform: Optional[str] = None
    source_port: Optional[str] = None

@dataclass
class Transformation:
    name: str
    type: str
    reusable: bool = False
    ports: List[Port] = field(default_factory=list)
    attributes: Dict[str, str] = field(default_factory=dict)
    origin: str = ""
    database_name: str = ""          # logical database name (e.g., OLTP, OLAP)
    connection_info: str = ""        # for lookup: e.g., $Target, $Source, or literal

@dataclass
class Connector:
    from_instance: str
    from_field: str
    from_instance_type: str
    to_instance: str
    to_field: str
    to_instance_type: str

@dataclass
class Mapping:
    name: str
    transformations: Dict[str, Transformation] = field(default_factory=dict)
    connectors: List[Connector] = field(default_factory=list)
    variables: Dict[str, str] = field(default_factory=dict)

@dataclass
class Mapplet:
    name: str
    transformations: Dict[str, Transformation] = field(default_factory=dict)
    connectors: List[Connector] = field(default_factory=list)
    variables: Dict[str, str] = field(default_factory=dict)

@dataclass
class Folder:
    name: str
    sources: Dict[str, Transformation] = field(default_factory=dict)
    targets: Dict[str, Transformation] = field(default_factory=dict)
    reusable_transformations: Dict[str, Transformation] = field(default_factory=dict)
    mapplets: Dict[str, Mapplet] = field(default_factory=dict)
    mappings: Dict[str, Mapping] = field(default_factory=dict)

@dataclass
class Repository:
    name: str
    folders: Dict[str, Folder] = field(default_factory=dict)

# ----------------------------------------------------------------------
# Main parser
# ----------------------------------------------------------------------
class PowerCenterParser:
    def __init__(self):
        self.repo = Repository(name="")

    def parse(self, xml_path: str) -> Repository:
        tree = etree.parse(xml_path)
        root = tree.getroot()
        repo_elem = root.find("REPOSITORY")
        if repo_elem is None:
            raise ValueError("Missing REPOSITORY element")
        self.repo.name = repo_elem.get("NAME", "Unknown")

        for folder_elem in repo_elem.findall("FOLDER"):
            folder_name = folder_elem.get("NAME", "Unknown")
            folder = Folder(name=folder_name)
            self._parse_folder_contents(folder_elem, folder)
            self.repo.folders[folder_name] = folder
        return self.repo

    def _parse_folder_contents(self, folder_elem, folder: Folder):
        # Sources
        for src_elem in folder_elem.findall("SOURCE"):
            t = self._parse_source_or_target(src_elem, origin="source")
            folder.sources[t.name] = t
        # Targets
        for tgt_elem in folder_elem.findall("TARGET"):
            t = self._parse_source_or_target(tgt_elem, origin="target")
            folder.targets[t.name] = t
        # Reusable transformations
        for trans_elem in folder_elem.findall("TRANSFORMATION"):
            t = self._parse_transformation(trans_elem, origin="reusable")
            folder.reusable_transformations[t.name] = t
        # Mapplets
        for mpl_elem in folder_elem.findall("MAPPLET"):
            mpl = self._parse_mapplet(mpl_elem)
            folder.mapplets[mpl.name] = mpl
        # Mappings
        for map_elem in folder_elem.findall("MAPPING"):
            mapping = self._parse_mapping(map_elem)
            folder.mappings[mapping.name] = mapping

    def _parse_source_or_target(self, elem, origin: str) -> Transformation:
        name = elem.get("NAME", "")
        t = Transformation(name=name, type=f"{origin.capitalize()} Definition", origin=origin)
        t.attributes = dict(elem.attrib)
        t.database_name = elem.get("DBDNAME", "")
        field_tag = "SOURCEFIELD" if origin == "source" else "TARGETFIELD"
        for field_elem in elem.findall(field_tag):
            port = Port(
                name=field_elem.get("NAME", ""),
                data_type=field_elem.get("DATATYPE", ""),
                precision=field_elem.get("PRECISION", ""),
                scale=field_elem.get("SCALE", ""),
                porttype=field_elem.get("PORTTYPE", "INPUT/OUTPUT"),
                default_value=field_elem.get("DEFAULTVALUE", ""),
                description=field_elem.get("DESCRIPTION", ""),
                parent_transform_name=name
            )
            t.ports.append(port)
        return t

    def _parse_transformation(self, elem, origin: str = "inline") -> Transformation:
        name = elem.get("NAME", "")
        ttype = elem.get("TYPE", "")
        reusable = elem.get("REUSABLE", "NO") == "YES"
        t = Transformation(name=name, type=ttype, reusable=reusable, origin=origin)
        t.attributes = dict(elem.attrib)

        for field_elem in elem.findall("TRANSFORMFIELD"):
            port = Port(
                name=field_elem.get("NAME", ""),
                data_type=field_elem.get("DATATYPE", ""),
                precision=field_elem.get("PRECISION", ""),
                scale=field_elem.get("SCALE", ""),
                porttype=field_elem.get("PORTTYPE", ""),
                default_value=field_elem.get("DEFAULTVALUE", ""),
                description=field_elem.get("DESCRIPTION", ""),
                expression=field_elem.get("EXPRESSION", ""),
                parent_transform_name=name
            )
            t.ports.append(port)

        for attr_elem in elem.findall("TABLEATTRIBUTE"):
            attr_name = attr_elem.get("NAME", "")
            attr_value = attr_elem.get("VALUE", "")
            t.attributes[attr_name] = attr_value
            if attr_name == "Connection Information":
                t.connection_info = attr_value
        return t

    def _parse_mapping(self, map_elem) -> Mapping:
        name = map_elem.get("NAME", "")
        mapping = Mapping(name=name)

        for trans_elem in map_elem.findall("TRANSFORMATION"):
            t = self._parse_transformation(trans_elem, origin="mapping_inline")
            mapping.transformations[t.name] = t

        for inst_elem in map_elem.findall("INSTANCE"):
            inst_name = inst_elem.get("NAME", "")
            if inst_name in mapping.transformations:
                continue
            trans_name = inst_elem.get("TRANSFORMATION_NAME", inst_name)
            trans_type = inst_elem.get("TRANSFORMATION_TYPE", "")
            t = Transformation(name=inst_name, type=trans_type, reusable=True, origin="mapping_instance")
            t.attributes["TRANSFORMATION_NAME"] = trans_name
            mapping.transformations[inst_name] = t

        for conn_elem in map_elem.findall("CONNECTOR"):
            connector = Connector(
                from_instance=conn_elem.get("FROMINSTANCE", ""),
                from_field=conn_elem.get("FROMFIELD", ""),
                from_instance_type=conn_elem.get("FROMINSTANCETYPE", ""),
                to_instance=conn_elem.get("TOINSTANCE", ""),
                to_field=conn_elem.get("TOFIELD", ""),
                to_instance_type=conn_elem.get("TOINSTANCETYPE", ""),
            )
            mapping.connectors.append(connector)

        for var_elem in map_elem.findall("MAPPINGVARIABLE"):
            var_name = var_elem.get("NAME", "")
            var_default = var_elem.get("DEFAULTVALUE", "")
            mapping.variables[var_name] = var_default
        return mapping

    def _parse_mapplet(self, mpl_elem) -> Mapplet:
        name = mpl_elem.get("NAME", "")
        mpl = Mapplet(name=name)

        for trans_elem in mpl_elem.findall("TRANSFORMATION"):
            t = self._parse_transformation(trans_elem, origin="mapplet_inline")
            mpl.transformations[t.name] = t

        for inst_elem in mpl_elem.findall("INSTANCE"):
            inst_name = inst_elem.get("NAME", "")
            if inst_name in mpl.transformations:
                continue
            trans_name = inst_elem.get("TRANSFORMATION_NAME", inst_name)
            trans_type = inst_elem.get("TRANSFORMATION_TYPE", "")
            t = Transformation(name=inst_name, type=trans_type, reusable=True, origin="mapplet_instance")
            t.attributes["TRANSFORMATION_NAME"] = trans_name
            mpl.transformations[inst_name] = t

        for conn_elem in mpl_elem.findall("CONNECTOR"):
            connector = Connector(
                from_instance=conn_elem.get("FROMINSTANCE", ""),
                from_field=conn_elem.get("FROMFIELD", ""),
                from_instance_type=conn_elem.get("FROMINSTANCETYPE", ""),
                to_instance=conn_elem.get("TOINSTANCE", ""),
                to_field=conn_elem.get("TOFIELD", ""),
                to_instance_type=conn_elem.get("TOINSTANCETYPE", ""),
            )
            mpl.connectors.append(connector)

        for var_elem in mpl_elem.findall("MAPPINGVARIABLE"):
            var_name = var_elem.get("NAME", "")
            var_default = var_elem.get("DEFAULTVALUE", "")
            mpl.variables[var_name] = var_default
        return mpl

    # ------------------------------------------------------------------
    # Resolve connections and propagate attributes
    # ------------------------------------------------------------------
    def resolve_connections_in_mapping(self, mapping: Mapping, folder: Folder):
        for inst_name, inst_trans in mapping.transformations.items():
            if inst_trans.origin == "mapping_instance":
                trans_name = inst_trans.attributes.get("TRANSFORMATION_NAME", "")
                ttype = inst_trans.type
                real_trans = None
                if ttype == "Source Definition":
                    real_trans = folder.sources.get(trans_name)
                elif ttype == "Target Definition":
                    real_trans = folder.targets.get(trans_name)
                elif ttype in ("Expression", "Lookup", "Joiner", "Filter", "Router", "Aggregator",
                               "Sequence", "Sorter", "Union", "Normalizer", "SQL Transformation",
                               "Stored Procedure", "Custom Transformation", "Mapplet"):
                    real_trans = folder.reusable_transformations.get(trans_name)
                if real_trans:
                    inst_trans.ports = []
                    for port in real_trans.ports:
                        new_port = Port(
                            name=port.name,
                            data_type=port.data_type,
                            precision=port.precision,
                            scale=port.scale,
                            porttype=port.porttype,
                            default_value=port.default_value,
                            expression=port.expression,
                            parent_transform_name=inst_name,
                        )
                        inst_trans.ports.append(new_port)
                    inst_trans.attributes.update(real_trans.attributes)
                    if real_trans.database_name:
                        inst_trans.database_name = real_trans.database_name
                    if real_trans.connection_info:
                        inst_trans.connection_info = real_trans.connection_info

        for conn in mapping.connectors:
            target_trans = mapping.transformations.get(conn.to_instance)
            if not target_trans:
                continue
            for port in target_trans.ports:
                if port.name == conn.to_field:
                    port.source_transform = conn.from_instance
                    port.source_port = conn.from_field
                    break

    def resolve_connections_in_mapplet(self, mapplet: Mapplet, folder: Folder):
        for inst_name, inst_trans in mapplet.transformations.items():
            if inst_trans.origin == "mapplet_instance":
                trans_name = inst_trans.attributes.get("TRANSFORMATION_NAME", "")
                ttype = inst_trans.type
                real_trans = None
                if ttype == "Source Definition":
                    real_trans = folder.sources.get(trans_name)
                elif ttype == "Target Definition":
                    real_trans = folder.targets.get(trans_name)
                elif ttype in ("Expression", "Lookup", "Joiner", "Filter", "Router", "Aggregator",
                               "Sequence", "Sorter", "Union", "Normalizer", "SQL Transformation",
                               "Stored Procedure", "Custom Transformation", "Mapplet"):
                    real_trans = folder.reusable_transformations.get(trans_name)
                if real_trans:
                    inst_trans.ports = []
                    for port in real_trans.ports:
                        new_port = Port(
                            name=port.name,
                            data_type=port.data_type,
                            precision=port.precision,
                            scale=port.scale,
                            porttype=port.porttype,
                            default_value=port.default_value,
                            expression=port.expression,
                            parent_transform_name=inst_name,
                        )
                        inst_trans.ports.append(new_port)
                    inst_trans.attributes.update(real_trans.attributes)
                    if real_trans.database_name:
                        inst_trans.database_name = real_trans.database_name
                    if real_trans.connection_info:
                        inst_trans.connection_info = real_trans.connection_info

        for conn in mapplet.connectors:
            target_trans = mapplet.transformations.get(conn.to_instance)
            if not target_trans:
                continue
            for port in target_trans.ports:
                if port.name == conn.to_field:
                    port.source_transform = conn.from_instance
                    port.source_port = conn.from_field
                    break

    def to_json(self) -> str:
        return json.dumps(self.repo, default=lambda o: o.__dict__, indent=2, ensure_ascii=False)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python powercenter_parser.py <xml_file>")
        sys.exit(1)
    parser = PowerCenterParser()
    repo = parser.parse(sys.argv[1])
    print(json.dumps(repo, default=lambda o: o.__dict__, indent=2, ensure_ascii=False))