import fdb 

conexao_destino = fdb.connect(dsn="localhost:C:\Fiorilli\SSEWEB\CIDADES\CAPIVARI\SECRETARIA.FDB", user='SYSDBA', 
                              password='masterkey', port=3050, charset='WIN1252')

conexao_origem = fdb.connect(dsn="localhost:C:\Conversao\CAPIVARI\BANCOSSE.FDB", user='SYSDBA',
                             password='masterkey', port=3050, charset='WIN1252')

def commit():
    conexao_destino.commit()
    
def get_cursor(conexao):
    return conexao.cursor()
