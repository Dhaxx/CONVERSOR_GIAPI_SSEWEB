NoneType = type(None)
from datetime import datetime
import conexao as cnx

cur_dest = cnx.get_cursor(cnx.conexao_destino)
cur_orig = cnx.get_cursor(cnx.conexao_origem)
cur_aux = cnx.get_cursor(cnx.conexao_aux)

def cadastro():
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

def atualizados():
    print("Cadastrando tramites...")
    insert = cur_dest.prep("""INSERT INTO se_ptramite (cod_emp_ptr, codigo_ptr, exercicio_ptr, item_ptr, setor_ant_ptr,
                                                       setor_atu_ptr, descricao_ptr, data_ptr, hora_ptr, relator_ptr, recebido_ptr,
                                                       aberto_ptr, login_inc_ptr, dta_inc_ptr, data_rec_ptr, ultimo, excluido_ptr)
                               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);""")

    cmd = cur_aux.execute("""SELECT a.ID N_PROCESSO, a.NUMERO_PROTOCOLO, a.PRCS_ANO_PROCESSO, g.*, b.UNID_SGL_UNIDADE  FROM "SIEM_PROCESSOS_1" a 
                              INNER JOIN "SIEM_ASSUNTOS_1" c ON a.ASSU_ID = c.ID 
                              INNER JOIN "SIEM_FUNCIONARIOS_1" d ON a.FUNC_ID = d.ID 
                              INNER JOIN "SIEM_UNIDADES_1" b ON a.UNID_ID = b.ID
                              INNER JOIN "SIEM_RESPONSABILIDADES_1" e ON a.ID = e.PROC_ID 
                              INNER JOIN "SIEM_PESSOAS_1" f ON e.PESS_ID = f.ID
                              INNER JOIN "SIEM_MOVIMENTACOES_1" g ON g.PROC_ID = a.ID 
                              WHERE b.INST_ID = 42 and a.NUMERO_PROTOCOLO > 301963 order by a.ID ;""") # ACERTAR O NUMERO DO PROTOCOLO

    for row in cmd.fetchall():
        cod_emp_ptr = 1
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
        data_ptr = row[4][:10]
        relator_ptr = 'CONVERSAO'
        recebido_ptr = 1
        aberto_ptr = 0
        login_inc_ptr = "CONVERSAO"
        dta_inc_ptr = row[4][:10]

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
