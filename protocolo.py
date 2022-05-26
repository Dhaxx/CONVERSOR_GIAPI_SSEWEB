from datetime import datetime
import conexao as cnx

cur_dest = cnx.get_cursor(cnx.conexao_destino)
cur_orig = cnx.get_cursor(cnx.conexao_origem)
cur_aux = cnx.get_cursor(cnx.conexao_aux)

def cadastro():
    print("Cadastrando protocolos...")

    insert = cur_dest.prep("""INSERT INTO se_pprotocolo (cod_emp_prt, codigo_prt, exercicio_prt, data_prt, hora_prt,
                                                         responsavel_prt, assunto_prt, dados_prt, login_inc_prt, dta_inc_prt,
                                                         login_alt_prt, dta_alt_prt, materia_tipo_prt, setor_prt, sigilo_prt,
                                                         interessado_prt,cod_asu_prt, chave_web_prt, tipo_prt, protocolo_prt,
                                                         pnumero_prt, pano_prt, ID_ANT, nome_int)
                              VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);""")

    cmd = cur_orig.execute("""SELECT a.ID, a.NUMERO_PROTOCOLO PROTOCOLO, a.PRCS_ANO_PROCESSO, a.PRCS_NUM_PROCESSO,
                              a.PRCS_DAT_ABERTURA, a.PRCS_HOR_ABERTURA, a.PRCS_DES_PROCESSO, a.PRCS_DAT_ARQUIVO, a.PRCS_HIS_PROCESSO,
                              a.PROC_DES_ENCERRAMENTO, a.PROC_DAT_ENCERRAMENTO, a.PROC_DAT_REABERTURA, a.PROC_DES_REABERTURA, a.UNID_ID,
                              a.ASSU_ID, c.ASTO_DES_ASSUNTO, a.FUNC_ID, d.MATR_NOM_FUNCIONARIO, a.PROC_DES_CORREDOR_ARQ, a.PROC_DES_PRATELEIRA_ARQ, a.PROC_DES_CAIXA_ARQ, a.PROC_STA_BAIXA,
                              a.PRCS_DES_FONETICA, a.PROC_NUM_INSCR, b.ID SEC, b.unid_des_unidade NOME, b.unid_sgl_unidade SIGLA, e.PESS_ID, f.PESS_NOM_PESSOA 
                              FROM "SIEM_CAPIVARI.SIEM_PROCESSOS_1" a 
                              INNER JOIN "SIEM_CAPIVARI.SIEM_ASSUNTOS_1" c ON a.ASSU_ID = c.ID 
                              INNER JOIN "SIEM_CAPIVARI.SIEM_FUNCIONARIOS_1" d ON a.FUNC_ID = d.ID 
                              INNER JOIN "SIEM_CAPIVARI.SIEM_UNIDADES_1" b ON a.UNID_ID = b.ID
                              INNER JOIN "SIEM_CAPIVARI.SIEM_RESPONSABILIDADES_1" e ON a.ID = e.PROC_ID 
                              INNER JOIN "SIEM_CAPIVARI.SIEM_PESSOAS_1" f ON e.PESS_ID = f.ID 
                              WHERE b.INST_ID = 42 order by NUMERO_PROTOCOLO;""")

    for row in cmd:
        cod_emp_prt = 1
        codigo_prt = row[1]
        exercicio_prt = row[2]
        data_prt = row[4].strftime("%Y-%m-%d")
        hora_prt = row[5]
        responsavel_prt = row[17]
        assunto_prt = row[15]
        dados_prt = row[6]
        login_inc_prt = "CONVERSAO"
        dta_inc_prt = data_prt
        login_alt_prt = "CONVERSAO"
        dta_alt_prt = data_prt
        materia_tipo_prt = "EXTERNA"
        setor_prt =  cur_dest.execute("SELECT cod_set FROM se_setor WHERE sigla_set = '" + row[26] + "'").fetchone()[0]
        sigilo_prt = "N"
        interessado_prt = str(row[27]).zfill(8)
        cod_asu_prt = cur_dest.execute("SELECT cod_asu FROM se_assunto WHERE descricao_asu = '" + row[15] + "'").fetchone()[0]
        chave_web_prt = None
        tipo_prt = 1
        protocolo_prt = str(row[1]).zfill(8)
        pnumero_prt = row[3]
        pano_prt = row[2]
        id_ant = row[0]
        nome_int = str(row[28])

        cur_dest.execute(insert,(cod_emp_prt, codigo_prt, exercicio_prt, data_prt, hora_prt, responsavel_prt, assunto_prt,
                       dados_prt, login_inc_prt, dta_inc_prt, login_alt_prt, dta_alt_prt, materia_tipo_prt, setor_prt,
                       sigilo_prt, interessado_prt, cod_asu_prt, chave_web_prt, tipo_prt, protocolo_prt, pnumero_prt, pano_prt, id_ant, nome_int))
    cnx.commit()

def atualizados(): # VERIFICAR A CNX DE ORIGEM
    print("Inserindo novos protocolos... ")

    insert = cur_dest.prep("""INSERT INTO se_pprotocolo (cod_emp_prt, codigo_prt, exercicio_prt, data_prt, hora_prt,
                                                         responsavel_prt, assunto_prt, dados_prt, login_inc_prt, dta_inc_prt,
                                                         login_alt_prt, dta_alt_prt, materia_tipo_prt, setor_prt, sigilo_prt,
                                                         interessado_prt,cod_asu_prt, chave_web_prt, tipo_prt, protocolo_prt,
                                                         pnumero_prt, pano_prt, ID_ANT, nome_int)
                              VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);""")

    cmd_aux = cur_aux.execute("""SELECT a.ID, a.NUMERO_PROTOCOLO PROTOCOLO, a.PRCS_ANO_PROCESSO, a.PRCS_NUM_PROCESSO,
                              a.PRCS_DAT_ABERTURA, a.PRCS_HOR_ABERTURA, a.PRCS_DES_PROCESSO, a.PRCS_DAT_ARQUIVO, a.PRCS_HIS_PROCESSO,
                              a.PROC_DES_ENCERRAMENTO, a.PROC_DAT_ENCERRAMENTO, a.PROC_DAT_REABERTURA, a.PROC_DES_REABERTURA, a.UNID_ID,
                              a.ASSU_ID, c.ASTO_DES_ASSUNTO, a.FUNC_ID, d.MATR_NOM_FUNCIONARIO, a.PROC_DES_CORREDOR_ARQ, a.PROC_DES_PRATELEIRA_ARQ, a.PROC_DES_CAIXA_ARQ, a.PROC_STA_BAIXA,
                              a.PRCS_DES_FONETICA, a.PROC_NUM_INSCR, b.ID SEC, b.unid_des_unidade NOME, b.unid_sgl_unidade SIGLA, e.PESS_ID, f.PESS_NOM_PESSOA 
                              FROM "SIEM_PROCESSOS_1" a 
                              INNER JOIN "SIEM_ASSUNTOS_1" c ON a.ASSU_ID = c.ID 
                              INNER JOIN "SIEM_FUNCIONARIOS_1" d ON a.FUNC_ID = d.ID 
                              INNER JOIN "SIEM_UNIDADES_1" b ON a.UNID_ID = b.ID
                              INNER JOIN "SIEM_RESPONSABILIDADES_1" e ON a.ID = e.PROC_ID 
                              INNER JOIN "SIEM_PESSOAS_1" f ON e.PESS_ID = f.ID 
                              WHERE b.INST_ID = 42 AND NUMERO_PROTOCOLO > 301963 order by NUMERO_PROTOCOLO""") # ACERTAR O NUMERO DO PROTOCOLO

    for row in cmd_aux:
        cod_emp_prt = 1
        codigo_prt = row[1]
        exercicio_prt = row[2]
        data_prt = row[4].strftime("%Y-%m-%d")
        hora_prt = row[5]
        responsavel_prt = row[17]
        assunto_prt = row[15]
        dados_prt = row[6]
        login_inc_prt = "CONVERSAO"
        dta_inc_prt = data_prt
        login_alt_prt = "CONVERSAO"
        dta_alt_prt = data_prt
        materia_tipo_prt = "EXTERNA"
        setor_prt =  cur_dest.execute("SELECT cod_set FROM se_setor WHERE sigla_set = '" + row[26] + "'").fetchone()[0]
        sigilo_prt = "N"
        interessado_prt = str(row[27]).zfill(8)
        cod_asu_prt = cur_dest.execute("SELECT cod_asu FROM se_assunto WHERE descricao_asu = '" + row[15] + "'").fetchone()[0]
        chave_web_prt = None
        tipo_prt = 1
        protocolo_prt = str(row[1]).zfill(8)
        pnumero_prt = row[3]
        pano_prt = row[2]
        id_ant = row[0]
        nome_int = str(row[28])

        cur_dest.execute(insert,(cod_emp_prt, codigo_prt, exercicio_prt, data_prt, hora_prt, responsavel_prt, assunto_prt,
                       dados_prt, login_inc_prt, dta_inc_prt, login_alt_prt, dta_alt_prt, materia_tipo_prt, setor_prt,
                       sigilo_prt, interessado_prt, cod_asu_prt, chave_web_prt, tipo_prt, protocolo_prt, pnumero_prt, pano_prt, id_ant, nome_int))
    cnx.commit()
