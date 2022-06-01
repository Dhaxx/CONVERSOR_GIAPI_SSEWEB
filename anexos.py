import conexao as cnx

cur_dest = cnx.get_cursor(cnx.conexao_destino)

def inserir():
    print("Inserindo Anexos...")
    
    insert = cur_dest.prep("""INSERT INTO SE_PDOCUMENTOS(COD_EMP_PDC,ID_PDC,CODIGO_PDC,EXERCICIO_PDC,NUMERO_PDC,PAG_PDC,LOGIN_INC_PDC,
	                                                    dta_inc_pdc, arquivo_pdc, tipodoc_pdc, ano_pdc, person_pdc, idperson_pdc, sigilo_pdc)
                                          VALUES (?,?,?,?,?,?,?,
                                                  ?,?,?,?,?,?,?)""")

    select = cur_dest.execute("""SELECT a.CODIGO_PRT, a.DTA_INC_PRT, a.PANO_PRT, (a.CODIGO_PRT||'-'||a.PANO_PRT) AS Cod  FROM SE_PPROTOCOLO a 
                                 INNER JOIN ANEXOS b ON a.CODIGO_PRT = b.protocolo AND a.PANO_PRT = b.ano""")

    i = 12
    for row in select.fetchall():
        i += 1
        cod_emp_pdc = 1
        id_pdc = i
        codigo_pdc = row[0]
        exercicio_pdc = row[2]
        numero_pdc = 1 
        pag_pdc = 1
        login_inc_pdc = "LARISSA.CAMPOS"
        dta_inc_pdc = row[1]
        arquivo_pdc = row[3]
        tipodoc_pdc = "PROTOCOLO/PROCESSO"
        ano_pdc = row[2]
        person_pdc = "N"
        idperson_pdc = 0
        sigilo_pdc = "N"

        cur_dest.execute(insert,(cod_emp_pdc,id_pdc,codigo_pdc,exercicio_pdc,numero_pdc,pag_pdc,login_inc_pdc,
	                             dta_inc_pdc, arquivo_pdc, tipodoc_pdc, ano_pdc, person_pdc, idperson_pdc, sigilo_pdc))
    cnx.commit()