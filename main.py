
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
import sys

def main(*args):
    if args[0] == '1':
        return at.main()
        
<<<<<<< Updated upstream
    if args[0] == '2':
        #return consulta.main()
        pass 

if __name__ == '__main__':
    tabla = main('1')
    print(tabla.head(20))
=======
    elif args[0] == '2':
        tabla = cz.main()
        # sql.zfer_update(tabla)
        return tabla

if __name__ == '__main__':
    tabla = main('2')
    # tabla = main(sys.argv[1])
>>>>>>> Stashed changes
