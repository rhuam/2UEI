# Biblioteca para Manipulação do MongoDB

Essa biblioteca tem como função facilitar a importação de contextos para o nosso banco de dados local e inserir os dados geográficos de cada contexto.
A seguir será descrito alguns comandos úteis. Mais comandos podem ser consultados no [Manual do MongoDB](https://docs.mongodb.com/manual/).

### Iniciando a conexão

Embora a conexção seja simples usando o mongoDB, o objeto dict2mongo faz a conexão sozinho quando instânciado.
```
obj = dict2mongo('twint')
```

O único parametro exirgido é o nome do banco de dados que será usado. É obrigado que o banco exista, caso contrário será criado um novo banco.
Os bancos existentes são:

- **twint**: Banco com todos os tweets.
- **context**: Banco com todos os contextos extraídos (Jogos, Climas)

### Principais métodos

O objeto criado, possui os seguintes métodos:

```
obj.jogoContext(filePath)
obj.climaContext(filePath)
```
Para converter arquivos .csv para o formado JSON e salvar no banco instânciado.

```
obj.tweetContext(filePath)
```
Para salvar arquivos ou consultas JSON no banco instânciado.

```
obj.simpleQuery(typeCoordinates, coordinates, limit, startDate,endDate )
```
Um consulta simples apenas para conhecer a sintaxe de consultas.

### Outros métodos

Outros métodos podem ser usados de acordo com o [Manual do MongoDB](https://docs.mongodb.com/manual/). Para isso basta usar a propriedade 'db' do objeto:

```
obj.db.find({})
```
