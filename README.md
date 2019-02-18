# Veficacao-de-log
Verificação de Log 

"""
HTTP requests to the NASA Kennedy Space Center WWW server
Fonte oficial do dateset: http://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html
Dados:
● Jul 01 to Jul 31, ASCII format, 20.7 MB gzip compressed, 205.2 MB.
● Aug 04 to Aug 31, ASCII format, 21.8 MB gzip compressed, 167.8 MB.

Sobre o dataset: Esses dois conjuntos de dados possuem todas as requisições HTTP para o servidor da NASA Kennedy
Space Center WWW na Flórida para um período específico.
Os logs estão em arquivos ASCII com uma linha por requisição com as seguintes colunas:
● Host fazendo a requisição. 
Um hostname quando possível, caso contrário o endereço de internet se o nome
não puder ser identificado.
● Timestamp no formato "DIA/MÊS/ANO:HH:MM:SS TIMEZONE"
● Requisição (entre aspas)
● Código do retorno HTTP
● Total de bytes retornados
Questões
Responda as seguintes questões devem ser desenvolvidas em Spark utilizando a sua linguagem de preferência.
1. Número de hosts únicos.
2. O total de erros 404.
3. Os 5 URLs que mais causaram erro 404.
4. Quantidade de erros 404 por dia.
5. O total de bytes retornados.

Solução abaixo:
"""

#Base de Dados:

	#  Agosto-> NASA_access_log_Aug95.gz 
  
	#  Julho -> NASA_access_log_Jul95.gz
  
	
   from pyspark import SparkConf, SparkContext
   from operador import add
	
	conf = (SparkConf ()
	         .setMaster ( " local " )
	         .setAppName ( " SPark " )
	         .set ( " spark.executor.memory " , " 2g " ))
	sc = SparkContext ( conf  = conf)
	

    agosto = sc.textFile ( ' access_log_Aug95 ' )
	agosto = agosto.cache ()

	julho = sc.textFile (" access_log_Jul95 ")
	julho = julho.cache ()
	
	# número de hosts distintos
	agosto_count = agosto.flatMap ( linha lambda  : line.split ( ' ' ) [ 0 ]). distinct (). count ()
	july_count = july.flatMap ( linha lambda  : line.split ( ' ' ) [ 0 ]). distinct (). count ()
	
    print ( ' Hosts distintos em agosto de % s '  % august_count)
	print ( ' Hosts distintos em julho: % s '  % july_count)
	
	# número de erros 404
	def erros_404 ( linha ):
	    tente :
	        code = line.split ( '  ' ) [ - 2 ]
	        se código ==  ' 404 ' :
	            return  True
	    exceto :
	        passar
	    return  False
	    
	julio_404 = july.filter (erros_404) .cache ()
	agosto_404 = agosto.filter ( linha lambda  : line.split ( ' ' ) [ - 2 ] == ' 404 ' ) .cache ()
	print ( ' 404 erros ocorridos em julho: % s '  % julio_404.count ())
	print ( ' erros 404 ocorridos em agosto % s '  % agosto_404.count ())
	
	#Os 5 URLs que mais causaram erro 404
	
	def  endereco_Inter_mais_erros ( rdd ):
	    endpoints = rdd.map ( linha lambda  : line.split ( ' " ' ) [ 1 ] .split ( ' ' ) [ 1 ])
	    contagem = endpoints.map ( lambda  endpoint : (endpoint, 1 )). reduceByKey (adicionar)
	    top = counts.sortBy ( par lambda  : - par [ 1 ]) take ( 5 )
	    
	    print ( ' \ n Os 5 URLs que mais causaram erro 404: ' )
	    par endpoints, conte no topo:
	        imprimir (ponto final, contagem)
	        
	    voltar topo
	endereco_Inter_mais_erros (julho_404)
	endereco_Inter_mais_erros (agosto_404)
	
	# 404 erros por dia
	def  Contagem_de_dias( rdd ):
	    days = rdd.map ( linha lambda  : line.split ( ' [ ' ) [ 1 ] .split ( ' : ' ) [ 0 ])
	    counts = days.map ( dia lambda  : (dia, 1 )). reduceByKey (adicionar) .collect ()
	    
	    print ( ' \ n 404 erros por dia: ' )
	    por dia, conte em contagens:
	        imprimir (dia, contar)
	        
	    contagens de retorno
	

	Contagem_de_dias(julho_404)
	Contagem_de_dias(agosto_404)
	

	# total de bytes retornados 
	def  total_de_bytes_retornado ( rdd ):
	    def  contagem_byte ( linha ):
	        tente :
	            count =  int (line.split ( "  " ) [ - 1 ])
	            se contar <  0 :
	                raise  ValueError ()
	            contagem_byte de retorno
	        exceto :
	            return  0
	        
	    count = rdd.map (contagem_byte) .reduce (adicionar)
	    contagem de retorno
	print ( ' Contagem total de bytes retornados  em julho: % s '  % total_de_bytes_retornado(julho))
	print ( ' Contagem total de bytes retornados  em agosto: % s '  % total_de_bytes_retornado(agosto))
	
	sc.stop ()

