from fb_scrape_public import getRangeQueryList
from neo4j.v1 import GraphDatabase, basic_auth #to connect to the database

#Dictionary of the Medios
dicMedios = {'Nacion'	:'115872105050',
			'CRHoy'		:'265769886798719',
			'Financiero':'47921680333',
			'Semanario'	:'119189668150973',
			'Tico Times':'124823954224180',
			'Extra'		:'109396282421232'}

#MediaTic's App's Info
AppID = '264737207353432'
AppSecret = '460c5a58dd6ddd6997b2645b1ad37cdd'


#queryList = getRangeQueryList(AppID,AppSecret,dicMedios['Financiero'],'El Financiero','','','2.10')
queryList = getRangeQueryList("264737207353432","460c5a58dd6ddd6997b2645b1ad37cdd","47921680333", "", "","","2.10")
#Connection with the database
driver = GraphDatabase.driver("bolt://localhost:7687", auth=basic_auth("neo4j", "neo4j"))
session = driver.session()

queryNumber = len(queryList)
queryCounter = 0
for query in queryList:
    session.run(query)
    queryCounter += 1
    print(queryCounter,' de ',queryNumber,' consultas ejecutadas')

session.close()

