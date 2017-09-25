#!/usr/bin/env python
# -*- coding: utf-8 -*-

def buildInsertNodeQuery(node_label, attribute_list):
    insertNodeQuery = ""
    insertNodeQuery += "CREATE (n:" + node_label + " " + getAttributes(attribute_list)
    insertNodeQuery += ")"
    return insertNodeQuery

#Para que en caso de que ya exista el nodo (porque ya su id está), solo lo actualice.
#IMPORTANTE attribute_list debe incluír el par (id: valor) de lo contrario habrá error en base de datos
def buildInsertOrUpdateNodeQuery(node_label, attribute_list):
    iuNodeQuery = ""
    iuNodeQuery += "MERGE (n:" + node_label + "{ id: '" +  attribute_list[0][1] + "'}) "
    attribute_list.pop(0)
    if attribute_list:
        iuNodeQuery += "SET n += " + getAttributes(attribute_list)
    return iuNodeQuery

#Metodo para establecer una relación entre dos nodos A - [r] -> B con los ids dados y con la lista de atributos dada
def buildInsertRelationshipQuery(relationship_name,label_A, id_A, label_B, id_B, attribute_list):
    insertRelationshipQuery = ""
    insertRelationshipQuery += "MATCH (a:" + label_A + " {id: '" + id_A + "'}), (b:" + label_B + "{id: '" + id_B + "'}) "
    insertRelationshipQuery += "CREATE (a)-[r:" + relationship_name + " " + getAttributes(attribute_list) + "]->(b)"
    return insertRelationshipQuery



def buildInsertOrUpdateRelationshipQuery(relationship_name,label_A, id_A, label_B, id_B, attribute_list):
    iuRelationshipQuery = ""
    iuRelationshipQuery += "MATCH (a:" + label_A + " {id: '" + id_A + "'}), (b:" + label_B + " {id: '" + id_B + "'}) "
    iuRelationshipQuery += "MERGE (a)-[r:" + relationship_name + "]-(b) SET r +=" + getAttributes(attribute_list)
    return iuRelationshipQuery

def getAttributes(attribute_list):
    attributes = "{"
    if attribute_list:
        attributes += attribute_list[0][0] + ": '" + attribute_list[0][1] + "'"
        iterattrs = iter(attribute_list)
        next(iterattrs) # Necesario para saltarse el primer elemento
        for pair in iterattrs:
            attribute_name = pair[0]
            attribute_value = pair[1]
            if not isinstance(attribute_value, int):
                attribute_value = "'" + attribute_value + "'"
            else:
                attribute_value = str(attribute_value)
            attributes += ", " + attribute_name + ":" + attribute_value
    attributes += "}"
    return attributes


#MATCH (a:Person {name: 'Juan'}), (b:Person {name: 'Carlos'}) MERGE (a)-[r:DETESTS]- (b) set r += {place:'TEHK'}
t = buildInsertNodeQuery("Post",[("id","12345"),("name","Nacion"),("fecha","enero"),("cantidad", 12),("lugar","Desampa")])
t1 = buildInsertOrUpdateNodeQuery("Post",[("id","12345"),("name","Nacion"),("fecha","enero"),("cantidad", 12),("lugar","Desampa")])
v = buildInsertRelationshipQuery("BELONGS_TO","Post","123","User","34",[("id","12345"),("name","Nacion"),("fecha","enero"),("cantidad", 12),("lugar","Desampa")])
at = getAttributes([("id","12345"),("name","Nacion"),("fecha","enero"),("cantidad", 12),("lugar","Desampa")])
r1 = buildInsertOrUpdateRelationshipQuery("BELONGS_TO","Post","123","User","34",[("id","12345"),("name","Nacion"),("fecha","enero"),("cantidad", 12),("lugar","Desampa")])

print(t1)
print(r1)