__author__ = 'fujita'
import mysql.connector

cnx=mysql.connector.connect(
    host='localhost',
    user='root',
    database='skei_sept',
    password='phdfromlse',
    buffered=True,
)
cnx.autocommit=True
cursor=cnx.cursor(buffered=True)


query="""SELECT
id,
networkId
FROM skei_sept.epg_pv
WHERE station_id_uni<=>NULL
"""

cursor.execute(query)

station_id_uni_replace={
    "0x7FE0":"001",
    "0x7FE2":"003",
    "0x7FE4":"005",
    "0x7FE5":"006",
    "0x7FE3":"004",
    "0x7FE6":"007"
}

updatelist=[]
for row in cursor:
    id=row[0]
    station_id_uni=station_id_uni_replace[row[1]]
    update={"id":id,
            "station_id_uni":station_id_uni}
    updatelist.append(update)

updatequery="""UPDATE skei_sept.epg_pv
SET station_id_uni=%(station_id_uni)s
WHERE id=%(id)s"""

cursor.executemany(updatequery,updatelist)
cnx.commit()

cursor.close()
cnx.close()
