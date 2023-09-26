
"""
This code collects and calculates the primary characteristics for a ZFER produced 
in AGP SGlass CO. It makes a recollection of all the critical variables in the 
different databases that the company has and combines them into a single dataframe 
that is to be exported to a SQL Server table. 

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
        loader.erase_table()
        loader.data_update(tabla)
        return tabla

    elif args[0] == '2':
        loader = Loader('SF_TiemposMecanizado_ZFER')
        loader.erase_table()
        tabla = cz.main()
        # loader.zfer_create(tabla)
        return tabla
    
    elif args[0] == '3':
        loader = Loader('SF_TiemposMecanizado_ZFER')
        tabla = az.main()
        loader.update_tablebyrow(tabla)
        return tabla

if __name__ == '__main__':
    tabla = main('3')
    # tabla = main(sys.argv[1])
