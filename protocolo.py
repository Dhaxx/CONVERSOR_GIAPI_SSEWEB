NoneType = type(None)
from datetime import datetime
import conexao as cnx

cur_dest = cnx.get_cursor(cnx.conexao_destino)
cur_orig = cnx.get_cursor(cnx.conexao_origem)

def cadastro():
    cur_dest.execute("delete from se_pprotocolo")
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
        data_prt = row[4].strftime('%Y-%m-%d')
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
        #interessado_prt =  None if row[27] == "" else cur_dest.execute("SELECT cod_cnt FROM gr_contribuintes WHERE nome_cnt = '" + row[27] + "'").fetchone()[0]
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

def tramites():
    print("Cadastrando tramites...")
    insert = cur_dest.prep("""INSERT INTO se_ptramite (cod_emp_ptr, codigo_ptr, exercicio_ptr, item_ptr, setor_ant_ptr,
                                                       setor_atu_ptr, descricao_ptr, data_ptr, hora_ptr, relator_ptr, recebido_ptr,
                                                       aberto_ptr, login_inc_ptr, dta_inc_ptr, data_rec_ptr, ultimo, excluido_ptr)
                               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);""")

    cmd = cur_orig.execute("""SELECT a.ID N_PROCESSO, a.NUMERO_PROTOCOLO, a.PRCS_ANO_PROCESSO, g.*, b.UNID_SGL_UNIDADE  FROM "SIEM_CAPIVARI.SIEM_PROCESSOS_1" a 
                              INNER JOIN "SIEM_CAPIVARI.SIEM_ASSUNTOS_1" c ON a.ASSU_ID = c.ID 
                              INNER JOIN "SIEM_CAPIVARI.SIEM_FUNCIONARIOS_1" d ON a.FUNC_ID = d.ID 
                              INNER JOIN "SIEM_CAPIVARI.SIEM_UNIDADES_1" b ON a.UNID_ID = b.ID
                              INNER JOIN "SIEM_CAPIVARI.SIEM_RESPONSABILIDADES_1" e ON a.ID = e.PROC_ID 
                              INNER JOIN "SIEM_CAPIVARI.SIEM_PESSOAS_1" f ON e.PESS_ID = f.ID
                              INNER JOIN "SIEM_CAPIVARI.SIEM_MOVIMENTACOE" g ON g.PROC_ID = a.ID 
                              WHERE b.INST_ID = 42 order by a.ID ;""")

    for row in cmd.fetchall():
        cod_emp_ptr = 1
        # codigo_ptr = cur_dest.execute("SELECT codigo_prt FROM se_pprotocolo WHERE ID_ANT = " + str(row[8])).fetchone()[0]
        codigo_ptr = row[1]
        exercicio_ptr = row[2]
        item_ptr = row[13]
        setor_atu_ptr = cur_dest.execute("SELECT cod_set FROM se_setor WHERE sigla_set = '" + row[27] + "'").fetchone()[0]

        if row[22] == "null":
            setor_ant_ptr = setor_atu_ptr 
        else:
            cur_orig.execute("""SELECT a.*, b.UNID_SGL_UNIDADE,b.INST_ID  FROM "SIEM_CAPIVARI.SIEM_MOVIMENTACOE" a, "SIEM_CAPIVARI.SIEM_UNIDADES_1" b WHERE a.MOVI_UNID_ANTERIOR IS NOT NULL AND b.INST_ID = 42""")
            setor_ant_ptr = cur_dest.execute("SELECT cod_set FROM se_setor WHERE sigla_set = '" + str(row[27]) + "'").fetchone()[0]
        
        descricao_ptr = row[5] if row[5] is not None else None
        data_ptr = row[4].strftime('%Y-%m-%d')
        relator_ptr = 'CONVERSAO'
        recebido_ptr = 1
        aberto_ptr = 0
        login_inc_ptr = "CONVERSAO"
        dta_inc_ptr = row[4].strftime("%Y-%m-%d")

        if row[6] != "null":
            data_rec_ptr = datetime.strptime(row[6][:10], "%Y-%m-%d")
            hora_ptr = row[6][11:16] + ":00"
        else:
            data_rec_ptr = None
            hora_ptr = "00:00:00"

        ultimo = 1
        excluido_ptr = 'N'

        cur_dest.execute(insert,(cod_emp_ptr, codigo_ptr, exercicio_ptr, item_ptr, setor_ant_ptr, setor_atu_ptr, descricao_ptr,
                       data_ptr, hora_ptr, relator_ptr, recebido_ptr, aberto_ptr, login_inc_ptr, dta_inc_ptr, data_rec_ptr, ultimo, excluido_ptr))
    print("FINALIZADO!")
    cnx.commit()

