
"""
This code contains the execution of 3 scripts that load SQL tables that relate
to the characteristics of production orders for laminated glass per lite.
This means that it produces a table that contains the machining time based on
the glass dimensions and its geometric differentials. 

The use of this code is exclusive for AGP Glass and cannot be sold or 
distributed to other companies. Unauthorized distribution of this code is a 
violation of AGP intellectual property.

Author: Juan Pablo Rodriguez Garcia (jpgarcia@agpglass.com)
"""
import scripts.actualizar_tabla as at
import scripts.calcular_zfer as cz
import scripts.actualizar_zfer as az
import sys
from sql import Loader

def main(*args):
    if args[0] == '1':
        loader = Loader('SF_TiemposMecanizado')
        tabla = at.main()
        loader.borrar_datos_antiguos()
        loader.cargar_datos(tabla)
        return tabla

    elif args[0] == '2':
        loader = Loader('SF_TiemposMecanizado_ZFER')
        loader.borrar_datos_antiguos()
        tabla = cz.main()
        loader.cargar_datos(tabla)
        return tabla
    
    elif args[0] == '3':
        loader = Loader('SF_TiemposMecanizado_ZFER')
        tabla = az.main()
        loader.update_tablebyrow(tabla)
        return tabla

if __name__ == '__main__':
    tabla = main('3')
    # tabla = main(sys.argv[1])
