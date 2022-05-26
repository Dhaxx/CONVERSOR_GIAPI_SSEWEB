import conexao as cnx
from conexao import conexao_destino

cur_dest = cnx.get_cursor(conexao_destino)

def limpa_tabelas():
    print("Limpando tabelas...")
    tabelas = ["delete from SE_PDOCUMENTOS",
               "delete from SE_CARGOS",
               "delete from SE_ASSUNTO",
               "delete from SE_PTRAMITE",
               "delete from SE_PPROTOCOLO",
               "delete from GR_CONTRIBUINTES",
               "delete from GR_LOGRA",
               "delete from GR_BAIRRO",
               "delete from GR_CIDADE",
               "delete from SE_SSE_PERMISSAO WHERE COD_USR_PER > 4",
               "delete from SE_USER_SETOR",
               "delete from SE_SSE_USUARIO",
               "delete from SE_SETOR",
               "delete from SE_TIPODOC"]

    for tabela in tabelas:
        cur_dest.execute(tabela)      
    cnx.commit()

def empresa():
    return int(conexao_destino.execute("SELECT cod_emp FROM gr_cad_empresa GCE").fetchone()[0])

def cria_campo(nome_tabela, nome_campo):
    resultado = conexao_destino.execute(   # Aqui realiza-se uma verificação para validar se a coluna em questão já é existente
        """select count(*) from rdb$relation_fields 
           where rdb$relation_name = '{tabela}' and(rdb$field_name = '{campo}')""".format(tabela=nome_tabela.upper(), 
                                                                                   campo=nome_campo.upper())).fetchone()[0]

    if resultado == 0: return

    cur_dest.execute("alter table {tabela} add {campo} varchar(80)".format(
            tabela=nome_tabela, campo=nome_campo))  
    cnx.commit()

def cria_usuario():
    cur_dest.execute("""INSERT INTO SE_SSE_USUARIO (COD_USR, COD_EMP_USR, NOME_USR, SENHA_USR, TIPO_USR, LOGIN_USR, PERMISSAO_USR, CARGO_USR, MODSEC, MODPRO, MODARQ, MODAGD, SIGILO_USR, RELATORIOS_USR, INATIVO, DIGITALIZACAO, USERNAME_SIA_USR, GRUPO_USR, COD_GRUPO_USR, APENSO_USR, PESQUISA_ABRIR_USR, AJUSTECOD_SIA, USERCODE_SIA_USR, "FOTO", HORAI, HORAF, DIASEG, DIATER, DIAQUA, DIAQUI, DIASEX, DIASAB, DIADOM, MUDARSENHA_USR, ASSTRAMITE_USR, PUBLICACI_USR, CERT_ID_USR, CERT_CERTIFICADORA_USR, CERT_VENCIMENTO_USR, CERT_SERIAL_USR, CERT_RAZAOSOCIAL_USR, CERT_CNPJ_USR, CERT_ARQUIVO_USR, CERT_ASSINAR_USR, CERT_SENHA_USR, EMAIL_USR) VALUES(9, 1, '', 'pi', '01 - Usuário', '@AMENDOLA', NULL, '', '1', '1', '1', '1', 'S', 'S', 'N', 'S', NULL, NULL, NULL, 'S', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'N', NULL, 'N', NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'N', NULL, NULL)""")
    cnx.commit()