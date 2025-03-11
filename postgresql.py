import psycopg2
from configuracion import leerINI
from datetime import datetime as dt

FICHERO_INI = "D:\\Mis documentos\\ProyectoA\\Python\\postgresql\\con.conf"
SECCION_INI_BD = "postgresql"

# Conectar al servidor de base de datos PostgreSQL
# Se le pasa un diccionario con la clave:valor de los datos de conexión
def conectarBDPosgreSQL(configuracion):
    try:
        # Conectamos al servidor de BD PostgreSQL indicando en en fichero INI
        with psycopg2.connect(host=configuracion.get("servidor"),
                              dbname=configuracion.get("base_datos"),
                              user=configuracion.get("usuario"),
                              password=configuracion.get("contrasena"),
                              port=configuracion.get("puerto")) as conexion:
            print("Conectado correctamente al servidor PostgreSQL [{0}:{1}]".format(
                configuracion.get("servidor"), configuracion.get("puerto")))
            return conexion
    except (psycopg2.DatabaseError, Exception) as error:
        print(error)

if __name__ == '__main__':
    # Leemos la configuración de conexión en el fichero INI
    configuracion = leerINI(ficheroINI=FICHERO_INI, seccion=SECCION_INI_BD)
    # Conectamos al servidor con la configuración del fichero INI
    conexion = conectarBDPosgreSQL(configuracion)
    # Activamos autocommit para no tener que hacer un commit en cada sentencia de modificación
    conexion.autocommit = True
    
    # Eliminamos la tabla si existe
    # Atención con este proceso pues puede eliminar una tabla completa existente y todos sus datos
    sqlEliminarTabla = "DROP TABLE IF EXISTS factura"
    
    # Preparamos las consultas SQL que usaremos
    sqlCrearTabla = """
        CREATE TABLE factura
        (
            numero serial PRIMARY KEY,
            codigo_cliente integer,
            importe money,
            fecha date
        );
        """

    dia = dt.now().day
    mes = dt.now().month
    ano = dt.now().year
    sqlInserts = (f"INSERT INTO factura (codigo_cliente, importe, fecha) VALUES (11, 1555.21, to_timestamp('{dia}-{mes}-{ano}', 'dd-mm-yyyy'))",
                  f"INSERT INTO factura (codigo_cliente, importe, fecha) VALUES (10, 100.5,  to_timestamp('{dia}-{mes}-{ano}', 'dd-mm-yyyy'))",
                  f"INSERT INTO factura (codigo_cliente, importe, fecha) VALUES (11, 1000,  to_timestamp('{dia}-{mes}-{ano}', 'dd-mm-yyyy'))",
                  f"INSERT INTO factura (codigo_cliente, importe, fecha) VALUES (20, 10.95,  to_timestamp('{dia}-{mes}-{ano}', 'dd-mm-yyyy'))",
                  f"INSERT INTO factura (codigo_cliente, importe, fecha) VALUES (33, 10.95,  to_timestamp('{dia}-{mes}-{ano}', 'dd-mm-yyyy'))")
        
    sqlSelect = "SELECT * FROM factura ORDER BY fecha DESC"
    
    sqlUpdate = "UPDATE factura SET importe = 5000 WHERE codigo_cliente=20"
    
    sqlDelete = "DELETE FROM factura WHERE codigo_cliente = 33"
    
    # Iniciamos la ejecución de las consultas SQL
    with conexion.cursor() as cursor:
        # Eliminamos la tabla "factura" si ya existe
        # Atención con este proceso pues puede eliminar una tabla completa existente y todos sus datos
        # Por precacuión, dejamos las líneas de eliminación comentadas
        """
        print("Eliminando tabla factura...")
        cursor.execute(sqlEliminarTabla)
        print("Tabla factura eliminada...")
        """
        
        # Creamos una tabla llamada "factura"
        print("Creando tabla factura...")
        cursor.execute(sqlCrearTabla)
        #conexion.commit()
        print("Tabla factura creada correctamente")
        
        # Insertamos algunos registros de ejemplo en la tabla factura
        # Recorremos la lista sqlInserts que contiene una sentencia Insert por cada elemento
        print("Insertando registros en tabla factura...")
        for sqlInsert in sqlInserts:
            cursor.execute(sqlInsert)
        print("Registros insertados correctamente en tabla factura...")            
        
        # Ejecutamos una consulta SQL de select (query) 
        print("Ejecutando query SQL en tabla factura...")
        cursor.execute(sqlSelect)
        # Mostramos el número de registros devuelto por la consulta SQL
        print("Número de facturas: ", cursor.rowcount)
        # Recorremos cada registro para mostrar sus datos por pantalla
        print("Registros de la tabla factura:")
        registro = cursor.fetchone()
        while registro is not None:
            # Mostramos el campo "codigo" y el campo "importe"
            print("Código: {0} Importe: {1}".format(registro[0], registro[2]))
            # Nos movemos al siguiente registro
            registro = cursor.fetchone()
        
        # Actualizamos (modificamos) un registro de la tabla
        print("Modificando registro en tabla factura...")
        cursor.execute(sqlUpdate)
        print("Registro modificado correctamente...")
        
        # Eliminamos un registro de la tabla
        print("Eliminando un registro en tabla factura...")
        cursor.execute(sqlDelete)
        print("Registro eliminado correctamente...")
        
        # Para finalizar, volvemos a mostrar los registros de la tabla (todos los campos)
        # Para comprobar que se ha actualizado un registro y se ha eliminado otro
        cursor.execute(sqlSelect)
        registro = cursor.fetchone()
        while registro is not None:
            print(registro)
            # Nos movemos al siguiente registro
            registro = cursor.fetchone()
        
        # Cerramos el cursor
        cursor.close