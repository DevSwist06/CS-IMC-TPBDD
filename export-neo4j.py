import os

import dotenv
import pyodbc
from py2neo import Graph
from py2neo.bulk import create_nodes, create_relationships
from py2neo.data import Node

dotenv.load_dotenv(override=True)

server = os.environ["TPBDD_SERVER"]
database = os.environ["TPBDD_DB"]
username = os.environ["TPBDD_USERNAME"]
password = os.environ["TPBDD_PASSWORD"]
driver= os.environ["ODBC_DRIVER"]

neo4j_server = os.environ["TPBDD_NEO4J_SERVER"]
neo4j_user = os.environ["TPBDD_NEO4J_USER"]
neo4j_password = os.environ["TPBDD_NEO4J_PASSWORD"]

graph = Graph(neo4j_server, auth=(neo4j_user, neo4j_password))

BATCH_SIZE = 10000

print("Deleting existing nodes and relationships...")
graph.run("MATCH ()-[r]->() DELETE r")
graph.run("MATCH (n:Artist) DETACH DELETE n")
graph.run("MATCH (n:Film) DETACH DELETE n")

with pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password) as conn:
    cursor = conn.cursor()

    # Films
    exportedCount = 0
    cursor.execute("SELECT COUNT(1) FROM TFilm")
    totalCount = cursor.fetchval()
    cursor.execute("SELECT idFilm, primaryTitle, startYear FROM TFilm")
    while True:
        importData = []
        rows = cursor.fetchmany(BATCH_SIZE)
        if not rows:
            break

        i = 0
        for row in rows:


            # Créer un objet Node avec comme label Film et les propriétés adéquates
            # A COMPLETER
            # Crée un nœud Neo4j avec le label "Film"
            # Les propriétés sont extraites des colonnes SQL :
            # - row[0] = idFilm (identifiant unique du film)
            # - row[1] = primaryTitle (titre principal du film)
            # - row[2] = startYear (année de sortie du film)
            n = Node("Film", idFilm=row[0], primaryTitle=row[1], startYear=row[2])
            # Ajoute le nœud créé à la liste importData pour traitement en lot


            importData.append(n)
            i += 1

        try:
            create_nodes(graph.auto(), importData, labels={"Film"})
            exportedCount += len(rows)
            print(f"{exportedCount}/{totalCount} title records exported to Neo4j")
        except Exception as error:
            print(error)

    # Names
    # En vous basant sur ce qui a été fait dans la section précédente, exportez les données de la table tArtist
    # A COMPLETER


    # Initialisation du compteur pour le suivi de la progression
    exportedCount = 0
    # Récupère le nombre total d'artistes dans la table tArtist
    cursor.execute("SELECT COUNT(1) FROM tArtist")
    totalCount = cursor.fetchval()
    # Requête SQL pour récupérer les données des artistes
    # idArtist : identifiant unique de l'artiste
    # primaryName : nom principal de l'artiste
    # birthYear : année de naissance de l'artiste
    cursor.execute("SELECT idArtist, primaryName, birthYear FROM tArtist")
    
    # Boucle de traitement par lots (BATCH_SIZE = 10000 lignes à la fois)
    while True:
        importData = []
        # Récupère le prochain lot de lignes
        rows = cursor.fetchmany(BATCH_SIZE)
        # Si aucune ligne n'est retournée, fin du traitement
        if not rows:
            break

        # Pour chaque ligne du lot
        for row in rows:
            # Crée un nœud Neo4j avec le label "Artist"
            # Les propriétés sont extraites des colonnes SQL :
            # - row[0] = idArtist (identifiant unique de l'artiste)
            # - row[1] = primaryName (nom principal de l'artiste)
            # - row[2] = birthYear (année de naissance de l'artiste)
            n = Node("Artist", idArtist=row[0], primaryName=row[1], birthYear=row[2])
            # Ajoute le nœud créé à la liste importData pour traitement en lot
            importData.append(n)

        try:
            # Insère tous les nœuds Artist du lot dans Neo4j
            # graph.auto() utilise l'objet Graph configuré
            # labels={"Artist"} spécifie le label des nœuds
            create_nodes(graph.auto(), importData, labels={"Artist"})
            # Mise à jour du compteur de nœuds exportés
            exportedCount += len(rows)
            # Affiche la progression
            print(f"{exportedCount}/{totalCount} artist records exported to Neo4j")
        except Exception as error:
            # Gestion des erreurs lors de l'insertion
            print(error)



    try:
        print("Indexing Film nodes...")
        graph.run("CREATE INDEX IF NOT EXISTS FOR (f:Film) ON (f.idFilm)")
        print("Indexing Name (Artist) nodes...")
        graph.run("CREATE INDEX IF NOT EXISTS FOR (a:Artist) ON (a.idArtist)")
    except Exception as error:
        print(error)


    # Relationships
    exportedCount = 0
    cursor.execute("SELECT COUNT(1) FROM tJob")
    totalCount = cursor.fetchval()
    cursor.execute(f"SELECT idArtist, category, idFilm FROM tJob")
    while True:
        importData = { "acted in": [], "directed": [], "produced": [], "composed": [] }
        rows = cursor.fetchmany(BATCH_SIZE)
        if not rows:
            break

        for row in rows:
            relTuple=(row[0], {}, row[2])
            importData[row[1]].append(relTuple)

        try:
            for cat in importData:
                # Utilisez la fonction create_relationships de py2neo pour créer les relations entre les noeuds Film et Name
                # (les tuples nécessaires ont déjà été créés ci-dessus dans la boucle for précédente)
                # https://py2neo.org/2021.1/bulk/index.html
                # ATTENTION: remplacez les espaces par des _ pour nommer les types de relation
                # A COMPLETER


                # Vérifie que la catégorie a des relations à créer
                if importData[cat]:
                    # Remplace les espaces par des underscores dans le nom de la relation
                    # Exemple : "acted in" devient "acted_in"
                    rel_type = cat.replace(" ", "_")
                    # Crée les relations en masse entre les nœuds Artist et Film
                    # importData[cat] contient les tuples (idArtist, {}, idFilm)
                    # start_node_key=("Artist", "idArtist") : le nœud source est un Artist identifié par idArtist
                    # end_node_key=("Film", "idFilm") : le nœud destination est un Film identifié par idFilm
                    # rel_type : le type de relation (ex: "acted_in", "directed", etc.)
                    create_relationships(graph.auto(), importData[cat], rel_type, start_node_key=("Artist", "idArtist"), end_node_key=("Film", "idFilm"))
            # Mise à jour du compteur de relations exportées


            exportedCount += len(rows)
            # Affiche la progression
            print(f"{exportedCount}/{totalCount} relationships exported to Neo4j")
        except Exception as error:
            # Gestion des erreurs lors de la création des relations
            print(error)
