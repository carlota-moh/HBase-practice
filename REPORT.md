# Práctica 2 - HBase

| Nombre | Email |
| --- | --- |
| Carlota Monedero Herranz | carlotamoh@alu.comillas.edu |
| Jorge Ayuso Martínez | jorgeayusomartinez@alu.comillas.edu |

En primer lugar, creamos el namespace donde procederemos a guardar nuestras tablas. Hemos escogido como nombre una combinación de nuestros usuarios (`mbd4` y `mbd25`):

```bash
create_namespace 'airdata_425'
```

Podemos comprobar que se ha creado correctamente:

```bash
list_namespace 'airdata_425'
```

# Ejercicio 1

**Detalle de aeropuertos: dado un código de aeropuerto se deben obtener todos sus atributos, con posibilidad de proyectar atributos específicos.**

En este caso, dado que la consulta más eficiente es mediante row-key, hemos decidido utilizar el código del aeropuerto como clave, aprovechando que se trata de un campo único para cada uno de los aeropuertos, además de que se encuentra ya establecido como una columna dentro del fichero `/tmp/nosql/airData/airports.csv`.

Aunque el resto de atributos podrían organizarse en múltiples column families diferentes, hemos decidido emplear una única column family, guardando los diferentes campos de información como qualifiers diferentes dentro de la misma. Esto se debe a que el número de qualifiers diferentes que vayan a estar presentes dentro de los datos es conocido y constante, además de que probablemente ello nos facilite la carga de datos, puesto que se trata de columnas dentro del csv que podemos cargar directamente. 

Para crear la tabla utilizamos el siguiente comando:

```bash
create 'airdata_425:airports', 'I'
```

Comprobamos que se ha creado correctamente:

```bash
list_namespace_tables 'airdata_425'
```

Una vez creada la tabla procedemos a realizar la carga de datos desde`/tmp/nosql/airData/airports.csv`. Para ello, utilizaremos el código de Python provisto en el script `1-airports.py`. Con el fin de poder ejecutar de manera limpia los scripts desde el nodo `Edge01`, así como evitar conflictos de dependencias, hemos creado un entorno en `conda` mediante el comando:

```bash
/opt/miniconda3/bin/conda create -n env_name python=3.8
```

Para poder ejecutar este entorno es necesario realizar un paso previo de inicialización de `conda` mediante el siguiente comando:

```bash
/opt/miniconda3/bin/conda
```

Tras cerrar la terminal para poder aplicar los cambios, volvemos a conectarnos al `Edge01` y observamos que en la consola aparece activado el entorno `base` de `conda`. Podemos ahora activar nuestro entorno personal mediante el comando que se muestra a continuación:

```bash
conda activate env_name
```

Una vez hecho esto, procedemos a instalar la librería de `happybase`:

```bash
pip install happybase
```

Finalmente, ejecutamos el script `1-airports.py` para poder realizar la carga de datos a la tabla recién creada. Tras ello, es necesario acceder a la shell de HBase (`hbase shell`) y ejecutar un scan de la tabla:

```bash
scan 'airdata_425:airports', {LIMIT => 3}
```

Con esto podemos comprobar que la estructura de datos que hemos especificado en la carga se mantiene en la tabla. Adicionalmente, para asegurarnos de que todos los registros han sido cargados, ejecutamos un `count`:

```bash
count 'airdata_425:airports'
```

Podemos ver que el número de filas (3376) coincide con el número de líneas que obtenemos si hacemos un `wc -l` sobre el fichero de `/tmp/nosql/airData/airports.csv`, lo que nos indica que todos los datos se han cargado en la tabla correctamente.

## Ejercicio 2

**Consulta de vuelos por mes o día: Posibilidad de obtener los vuelos de aun día o mes específico (YYYYMMDD o YYYYMM).** 

**Como detalle de información se deben obtener siempre todos los siguientes datos: hora de salida, hora de llegada, número de vuelo, origen, destino, número de aeronave y distancia recorrida.**

En este caso, queremos realizar una consulta de vuelos por días o mes específico, por lo que tenemos que idear una clave que sea combinación de las columnas que indican esta información en los `csv` de carga. Sin embargo, se puede comprobar que para un mismo día puede haber varios vuelos diferentes, por lo que no podemos utilizar únicamente esta información como row-key, puesto que nos encontraríamos con registros repetidos que no corresponden a los mismos vuelos. Podemos comprobar que esto es cierto con el siguiente comando en la terminal del `Edge01`:

```bash
cut -d ',' -f 1,2,3,10 /tmp/nosql/airData/2007.csv | grep '^2007,1,1,[0-9]*$' | wc -l
```

Este comando nos permite inspeccionar el fichero de vuelos de `/tmp/nosql/airData/2007.csv` seleccionando únicamente las columnas 1, 2, 3 y 10 (correspondientes al año, mes, día del mes y número de vuelo, respectivamente). En este caso, queremos buscar el número de entradas en el año 2007 para el mes de enero y el día 1. Para ello, empleamos comando `cut` de Linux, donde mediante el parámetro `-d` le especificamos el delimitador del `csv` (en este caso, comas), así como las columnas que queremos (mediante el parámetro `-f`). Sobre el output de este comando, realizamos una búsqueda `grep` para quedarnos con todas las entradas que correspondan al día 1 de enero de 2007, en cualquier número de vuelo. Finalmente, realizamos un `wc` del número de líneas del output del comando, resultando en un total de 19563, lo que nos confirma que no podemos utilizar únicamente la fecha como row-key. 

Una potencial aproximación para intentar resolver este problema es comprobar si en cada día el `id` del vuelo es un identificador único. Esto podría ayudarnos, ya que lo único que tendríamos que hacer es utilizar la información de la fecha como prefijo para la row-key y emplear el campo correspondiente al número de vuelo para hacer que el registro sea único. Para comprobar nuestra hipótesis, podemos utilizar un comando similar al anterior, pero indicando que se muestre el número de registros únicos, para lo cual es necesario añadir el comando `uniq` antes de hacer `wc -l`:

```bash
cut -d ',' -f 1,2,3,10 /tmp/nosql/airData/2007.csv | grep '^2007,1,1,[0-9]*$' | uniq | wc -l
```

Podemos observar que se obtiene un total de 17604, lo que nos indica que no podemos identificar un vuelo de forma unívoca haciendo uso únicamente del año, mes, día y número de vuelo. Con el siguiente comando se muestra aquellas combinaciones de año, mes, día y número de vuelo que tienen más de una coincidencia en el fichero `csv` de carga:

```bash
cut -d ',' -f 1,2,3,10 /tmp/nosql/airData/2007.csv | grep '^2007,1,1,[0-9]*$' | uniq -c | grep -v '^ *1'
```

Donde el comando `grep -v '^ *1'` indica que se muestren aquellos registros que no empiecen por 1.

Por ejemplo, si se selecciona específicamente un número de vuelo (en este caso se ha escogido el vuelo 2891, pero podría ser cualquier otro que tuviera más de una coincidencia el fichero `csv` de carga):

```bash
cut -d ',' -f 1,2,3,10 /tmp/nosql/airData/2007.csv | grep '^2007,1,1,2891$' | wc -l
```

Cuando ejecutamos este comando se puede observar que el número de entradas para ese vuelo en esa fecha específica es de 3, lo que indica que es posible que dicho vuelo tenga entradas repetidas. Para poder ver cuál es exactamente la diferencia entre dichas entradas, se necesita el resto de columnas del fichero para ese día y vuelo específico. El comando de filtrado a emplear es el siguiente:

```bash
egrep '^2007,1,1,[0-9,]*([A-Z]){2},2891' /tmp/nosql/airData/2007.csv
```

Con este comando podemos encontrar todas las entradas correspondientes al vuelo 2891 en el día 1 de enero de 2007. Podemos comprobar que aparecen dos entradas diferentes, puesto que se trata de un vuelo que fue desde Sacramento (SMF) hasta Los Angeles (ONT) y, tras una escala, se desplazó desde Los Angeles hasta Las Vegas (LAS). La inspección más detallada de las horas de aterrizaje de la primera entrada (13:41) y la hora de despegue de la segunda entrada (14:08) parecen corroborar nuestra hipótesis.

Con esta información en mente, se propone el diseño de una nueva clave que permita identificar registros de forma unívoca, es decir, que al hacer el conteo del número de registros únicos se obtenga el mismo valor que al hacer el conteo del número de registros totales. Tras analizar en detalle diferentes posibilidades, se ha decidido hacer uso de una clave que incluya la siguiente información:

- Año del vuelo (`YYYY`)
- Mes del vuelo (`MM`)
- Día del mes (`DD`)
- Compañía (`UniqueCarrier`)
- Número de vuelo (`FlightNum`)
- Origen (`Origin`)
- Destino (`Dest`)

Pese a ello, si se ejecutan los dos siguientes comandos:

```bash
# Mostrar número de líneas totales del fichero
wc -l /tmp/nosql/airData/2007.csv
```

```bash
# Número de registros únicos teniendo en cuenta los campos usados como row-key
cut -d ',' -f 1,2,3,9,10,17,18 /tmp/nosql/airData/2007.csv | uniq | wc -l
```

Podemos comprobar que no se obtiene el mismo resultados (7453216 en el primero y 7453186 en el segundo). Si analizamos más en detalle los para que casos hay registros repetidos:

```bash
cut -d ',' -f 1,2,3,9,10,17,18 /tmp/nosql/airData/2007.csv | uniq -c | grep -v '^ *1'
```

Podemos comprobar, si se analiza en detalle dichos casos, que se tratan de registros duplicados (es decir, registros que contienen exactamente la misma información). Por ello, podemos considerar la row-key propuesta como una opción válida en cuanto a que permite identificar registros de forma unívoca, pero tendremos que omitir los regitros duplicados cuando los datos se inserten en la tabla de HBase.

Otro potencial problema a tener en cuenta es que la longitud de la clave compuesta resultante es bastante elevado, puesto que se tiene que incluir información de varios campos para conseguir una clave única, lo que supone un consumo adicional de espacio de escritura en disco. Adicionalmente, los resultados se devolverán ordenados en función de estos campos: primero por año, después por mes, después por día, etc., por lo que si quisiéramos aplicar una ordenación diferente tendríamos que diseñar una clave distinta. Sin embargo, dado que no se tiene información adicional de cómo debe ser el orden de los resultados devueltos, se utilizará esta aproximación.

Una vez definido el tipo de clave a emplear se debe decidir el número de column families que se necesitan para poder almacenar la información de las entradas. Como en este caso se indica que siempre se desea acceder a todos los campos, lo más óptimo es emplear una única column family, de modo que se pueda realizar una búsqueda por column family de forma directa para obtener la información de todas las columnas.

Se procede por tanto a crear la nueva tabla dentro del namespace previamente definido:

```bash
create 'airdata_425:flights', 'info'
```

Comprobamos que se ha creado correctamente:

```bash
list_namespace_tables 'airdata_425'
```

A continuación, se procede a ejecutar el script `flights.py`, de manera similar a como lo hicimos en el ejercicio 1 (activando el entorno de `conda` previo a realizar la ejecución del script desde el nodo `Edge01`). Tras ello, se realizan las comprobaciones oportunas para ver que los datos se han insertado correctamente en nuestra tabla:

```bash
# Mostrar primeras filas de la tabla
scan 'airdata_425:flights', {LIMIT => 3}
```

```bash
# Conteo del número de regitros que contiene la tabla
count 'airdata_425:flights'
```