# ETL-dynamodb

Construimos un proceso ETL desde la creacion de una base de datos en DynamoDB la cual ingestamos de datos con una lambda, la cual ingresaba datos de peliculas a una cartelera de cine.

Para este proceso yo decidi elabora una funciòn de Lambda la cual se comunica con la base de datos, escanea la misma con una funcion y nos permite leer la categoria de pelicula mas recurrente en nuestra cartelera.

De igual forma la lambda incluye otra funcion donde mediante una lista de atributos puede leer si algun titulo dentro de la tabla posee uno de los listados y poder clasificarlo en la secciòn de "terror".

Mi elecciòn al utilizar esta herramienta fue basada en que Lambda nos permite invocar esta funciòn de una manera serverless, la cual no necesitamos de una instancia de computo para ejecutarse y solo pagar por el tiempo de ejecuciòn y memoria, y al ser una base de datos
con pocos elementos me parecio lo mas sensato. El tiempo de ejecucion es de 478 ms y la memoria utilizada fue de 89 MB.

Escalar este proceso a uno de big data quizas seria mejor en alguna otra herramienta como Glue o similar, que nos permite usar PySpark y otras herramientas como la visual.

Al finalizar la ejecucion, nuestros resultados son cargados en un bucket s3 en un formato JSON que nos permite leerse como una conclusion de que el proceso ETL llegò a lo indicado de una buena manera cumpliendo los requerimientos pedidos.


Gracias.

