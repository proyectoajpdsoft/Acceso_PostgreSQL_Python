from configparser import ConfigParser
import os.path

# Lee todos los valores de la sección pasada por 
# argumento de un fichero INI
def leerINI(ficheroINI="con.conf", seccion="postgresql"):
    parseador = ConfigParser()
    if os.path.exists(ficheroINI):        
        parseador.read(ficheroINI)

        # Obtenemos los datos de la sección postgresql (por defecto si no se ha especificado otra)
        # Los devolvemos en un diccionario (clave:valor)
        configINI = {}
        if parseador.has_section(seccion):
            valores = parseador.items(seccion)
            for valor in valores:
                configINI[valor[0]] = valor[1]
        else:
            raise Exception("No se ha encontrado la sección {0} en el fichero INI {1}".format(seccion, ficheroINI))
        return configINI
    else:
        raise Exception("No se ha encontrado el fichero INI {1}".format(ficheroINI))        

"""
if __name__ == "__main__":
    config = leerINI()
    print(configINI)
"""