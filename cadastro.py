import conexao as cnx

cur_dest = cnx.get_cursor(cnx.conexao_destino)
cur_orig = cnx.get_cursor(cnx.conexao_origem)

def cad_cargos():
    print("Cadastrando cargos...")
    insert = cur_dest.prep("INSERT INTO se_cargos(cod_emp_car, cod_car, descricao_car) VALUES (?,?,?) ")

    cur_orig.execute('SELECT * FROM "SIEM_CAPIVARI.SIEM_CARGOS_1" scsc')

    cod_car = 0
    for row in cur_orig:
        cod_emp_car = 1
        cod_car += 1
        descricao_car = row[1]

        cur_dest.execute(insert,(cod_emp_car, cod_car, descricao_car))
    cnx.commit()


def cad_setor():
    print("Cadastrando setores...")

    insert = cur_dest.prep("INSERT INTO se_setor (cod_emp_set, cod_set, descricao_set, sigla_set, login_inc_set) values (?, ?, ?, ?, ?)")

    cur_orig.execute("""SELECT a.ID, a.UNID_COD_UNIDADE,a.UNID_DES_UNIDADE,a.UNID_SGL_UNIDADE,
                        a.UNID_STA_ATIVO,a.INST_ID,a.UNID_STA_PROTOCOLO  
                        FROM "SIEM_CAPIVARI.SIEM_UNIDADES_1" a WHERE inst_id = 42 ORDER BY a.ID""") 

    i = 0

    for row in cur_orig:
        i += 1
        cod_emp_set = 1
        cod_set = i
        descricao_set = row[2]
        sigla_set = row[3]
        login_inc_set = "CONVERSAO"

        cur_dest.execute(insert,(cod_emp_set, cod_set, descricao_set, sigla_set, login_inc_set))
    cnx.commit()

def cad_assunto():
    print("Cadastrando assuntos...")

    insert = cur_dest.prep("INSERT INTO se_assunto(cod_emp_asu, cod_asu, descricao_asu, login_inc_asu, ativado_asu, mobile_asu) VALUES (?,?,?,?,?,?)")
    cur_orig.execute("""SELECT * FROM "SIEM_CAPIVARI.SIEM_ASSUNTOS_1" scsa""")

    i = 0
    for row in cur_orig:
        i += 1
        cod_emp_asu = 1
        cod_asu = i
        descricao_asu = row[1]
        login_inc_asu = "CONVERSAO"
        ativado_asu = row[2]
        mobile_asu = "N"

        cur_dest.execute(insert,(cod_emp_asu, cod_asu, descricao_asu, login_inc_asu, ativado_asu, mobile_asu))
    cnx.commit()

def tipo_doc():
    print("Inserindo tipos de documentos...")

    insert = cur_dest.prep("INSERT INTO se_tipodoc (cod_emp_tdc, cod_tdc, descricao_tdc, login_inc_tdc) VALUES(?, ?, ?, ?)")
    insert_protocolo = cur_dest.prep("INSERT INTO se_tipodoc (cod_emp_tdc, cod_tdc, descricao_tdc, login_inc_tdc) VALUES(1, 1, 'PROTOCOLO/PROCESSO', 'CONVERSAO');")
    cur_orig.execute("""SELECT * FROM "SIEM_CAPIVARI.SIEM_TIPOS_DOCUME" scstd """)

    i = 1

    cur_dest.execute(insert_protocolo)

    for row in cur_orig:
        i += 1
        cod_emp_tdc = 1
        cod_tdc = i
        descricao_tdc = row[1]
        login_inc_tdc = "CONVERSAO"

        cur_dest.execute(insert,(cod_emp_tdc, cod_tdc, descricao_tdc, login_inc_tdc))
    cnx.commit()

def cad_cidades():
    print("Cadastrando cidades...")

    insert = cur_dest.prep("INSERT INTO gr_cidade (cod_cid, nome_cid, uf_cid, popula_cid) VALUES (?, ?, ?, ?)")
    cur_orig.execute("""SELECT DISTINCT corr_nom_cidade  FROM "SIEM_CAPIVARI.SIEM_CORRESPONDEN" scsc """)

    i = 0

    for row in cur_orig:
        i += 1
        cod_cid = str(i).zfill(7)
        nome_cid = row[0].upper()
        uf_cid = "SP"
        populacao_cid = 0

        cur_dest.execute(insert,(cod_cid, nome_cid, uf_cid, populacao_cid))
    cnx.commit()

def cad_bairro():
    print("Cadastrando bairros...")

    insert = cur_dest.prep("INSERT INTO gr_bairro (cod_emp_bai, cod_bai, nome_bai) VALUES (?, ?, ?)")
    cur_orig.execute("""SELECT DISTINCT corr_nom_bairro  FROM "SIEM_CAPIVARI.SIEM_CORRESPONDEN" scsc""")

    cod_bai = 0

    for row in cur_orig:
        cod_emp_bai = 1
        cod_bai += 1
        nome_bai = row[0].upper()

        cur_dest.execute(insert,(cod_emp_bai, cod_bai, nome_bai))
    cnx.commit()

def cad_logradouro():
    print("Cadastrando logradouros...")

    insert = cur_dest.prep("INSERT INTO gr_logra(cod_emp_log, cod_log, cod_bair_log, cep_log, nome_log) VALUES (?, ?, ?, ?, ?)")
    cur_orig.execute("""SELECT * FROM "SIEM_CAPIVARI.SIEM_CORRESPONDEN" scsc ORDER BY ID""")

    for row in cur_orig:
        cod_emp_log = 1
        cod_log = row[0]
        cod_bair_log = cur_dest.execute("""SELECT cod_bai FROM gr_bairro WHERE nome_bai = '{}'""".format(row[7].upper())).fetchone()[0]
        cep_log = row[4]
        nome_log = row[1].upper() if len(row[1]) <= 60 else row[1][:60]

        cur_dest.execute(insert,(cod_emp_log, cod_log, cod_bair_log, cep_log, nome_log))
    cnx.commit()

def cad_contribuinte():
    cur_dest.execute("""delete from GR_CONTRIBUINTES""")
    print("Cadastrando contribuintes...")
    global cod_cnt_ant 
    cod_cnt_ant = ""

    insert = cur_dest.prep("""INSERT INTO gr_contribuintes(cod_emp_cnt, cod_cnt, cnpj_cnt,
                                                           nome_cnt, cod_log_cnt, nom_log_cnt,
                                                           numero_cnt, nom_bai_cnt, cep_cnt, 
                                                           cod_cid_cnt, nom_cid_cnt, rg_cnt, 
                                                           sexo_cnt, fisica_cnt, sequencial_cnt,
                                                           cnt_id_ant)
                              VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""")
    
    sequencial_cnt = 0

    cur_orig.execute("""SELECT
                            a.ID COD_PESSOA,
                            a.PESS_NOM_PESSOA NOME,
                            a.PESS_TIP_PESSOA TIPO,
                            a.PESS_NUM_CGC CNPJ,
                            a.PESS_NUM_RG RG,
                            a.PESS_SEXO SEXO,
                            b.ID COD_RUA,
                            b.CORR_DES_LOGRADOURO ENDERECO,
                            b.CORR_NOM_CIDADE CIDADE,
                            b.CORR_NUM_CEP CEP,
                            b.CORR_NOM_BAIRRO BAIRRO,
                            b.CORR_NUM_IMOVEL NUMERO
                        FROM "SIEM_CAPIVARI.SIEM_PESSOAS_1" a 
                        LEFT join "SIEM_CAPIVARI.SIEM_CORRESPONDEN" b ON a.ID = b.PESS_ID """)

    for row in cur_orig.fetchall():
        cod_emp_cnt = 1
        if row[0] != cod_cnt_ant:
            cod_cnt = str(row[0]).zfill(8)
        else:
            cod_cnt = str(int(cod_cnt) + 10000000).zfill(8)
        cnpj_cnt = row[3] if row[3] != "null" else None
        nome_cnt = row[1].upper()
        cod_log_cnt = row[6]

        if row[7] != None and len(row[7]) <= 70:
            nom_log_cnt = row[7] 
        elif row[7] != None and len(row[7]) > 70:
            nom_log_cnt = row[7][:70]
        else:
            nom_log_cnt = None

        numero_cnt = row[11]
        nom_bai_cnt = row[10]
        cep_cnt = row[9]
        nom_cid_cnt = row[8]
        rg_cnt = row[4] if row[4] != "null" else None
        fisica_cnt = row[2]
        sequencial_cnt += 1

        if nom_cid_cnt == None:
            cod_cid_cnt = None
        else:
            cod_cid_cnt = cur_dest.execute("""SELECT cod_cid FROM gr_cidade WHERE nome_cid = '{}'""".format(nom_cid_cnt)).fetchone()[0]
        
        
        if row[5] == "F":
            sexo_cnt = "Feminino"
        elif row[5] == "M":
            sexo_cnt = "Masculino"
        else:
            sexo_cnt = "Outro"
        cnt_cod_ant = row[0]

        cur_dest.execute(insert,(cod_emp_cnt, cod_cnt, cnpj_cnt, nome_cnt, cod_log_cnt,
                                 nom_log_cnt, numero_cnt, nom_bai_cnt, cep_cnt, cod_cid_cnt,
                                 nom_cid_cnt, rg_cnt, sexo_cnt, fisica_cnt, sequencial_cnt,
                                 cnt_cod_ant))
        cod_cnt_ant = row[0]
    cnx.commit()