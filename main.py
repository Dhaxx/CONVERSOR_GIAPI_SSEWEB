import utilitarios
import cadastro
import protocolo
import tramites

def main():
    #LIMPA TABELAS
    # utilitarios.limpa_tabelas()
    # utilitarios.cria_usuario()

    #CADASTROS AUXILIARES
    # cadastro.cad_cargos()
    # cadastro.cad_setor()
    # cadastro.cad_assunto()
    # cadastro.tipo_doc()
    # cadastro.cad_cidades()
    # cadastro.cad_bairro()
    # cadastro.cad_logradouro()
    # cadastro.cad_contribuinte()
    # protocolo.cadastro()
    #protocolo.atualizados()
    tramites.cadastro()
    tramites.atualizados() 

if __name__ == "__main__":
    main()