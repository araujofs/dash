# Definições de protocolos

As operações terão numeração continuada entre mensagens de controle e mensagens de dados.

A aplicação deve levar em conta que os bytes estão sendo enviados de maneira Big Endian.

## Controle

### Operações

- SUB (0): inscrever-se em tópico
- UNSUB (1): desinscrever-se de tópico
- ADD (2): criar tópico
- DEL (3): deletar tópico
- UPD_META (4): atualizar cache de metadados do cluster (servidor envia quando houver uma alteração)
- GET_META (5): atualizar cache de metadados do cluster (cliente envia quando precisar dos metadados sob demanda)
- PING (6): verificar se servidor/cliente ainda está ativo
- IDENTIFY (9): reconhecer/cadastrar um cliente pelo nome e retornar seu ID

### Reconhecimento do cliente (IDENTIFY)

- O cliente envia uma mensagem de controle com operação `IDENTIFY (9)`, tópico `0` e payload contendo apenas seu nome em UTF-8, de 1 a 255 bytes.
- O nome deve ser único entre os clientes cadastrados nesse servidor. Nome vazio, acima do limite ou já cadastrado resulta em falha.
- O servidor cadastra o cliente e atribui um ID `uint64` diferente de zero, usando sua sequência de clientes.
- A resposta também usa operação `IDENTIFY` e tópico `0`, mantendo o prefixo de tamanho das mensagens de controle.
- Payload de sucesso: `[status: 1 byte = 0][ID do cliente: 8 bytes, Big Endian]`.
- Payload de falha: `[status: 1 byte = 1][descrição do erro: UTF-8]`, sem ID.
- O ID identifica o cadastro enquanto o cliente permanecer conectado e pode ser reutilizado após sua remoção. Seu escopo é o servidor que o atribuiu.

### Decisões

- As mensagens (headers + payload) devem ser prefixadas por 4 bytes, delimitando o tamanho da mensagem
- Os headers das mensagens serão os seguintes (em ordem):
  - Operação - 1 byte
  - Tópico - 2 bytes (deve ser 0 em caso de a operação não precisar)
- Metadados do cluster devem ser enviados utilizando Protobuf, focando em eficiência de tamanho e velocidade, evitando grandes atrasos nas atualizações de metadados
- As seguintes operações devem retornar 0 para sucesso e 1 para falha:
  - SUB
  - UNSUB
  - ADD
  - DEL
  - UPD_META
  - PING
- SUB deve enviar também o nome do cliente fazendo a operação
  - Deve fazer isso mandando no payload o nome
- GET_META deve retornar e UPD_META deve enviar os seguintes dados:
  - Brokers ativos
    - Tópicos
      - Nome
      - Identificador
- O tamanho máximo de mensagem recebida pelo servidor deve ser de 1024B
- Cliente deve confiar no tamanho das respostas do servidor

## Dados

### Operações

- SEND (7): publicação de mensagem do publisher
- FORWARDING (8): encaminhamento de mensagem para consumers

### Decisões

- As mensagens serão lidas com 2048 bytes cada, o que passar disso será ignorado
- Os headers das mensagens serão os seguintes (em ordem):
  - Operação - 1 byte
  - Tópico - 2 bytes
  - ID do cliente remetente - 8 bytes (`uint64`, Big Endian)
  - Tamanho do nome do usuário - 1 byte, de 0 a 255, contando bytes UTF-8
  - Nome do usuário - quantidade de bytes indicada pelo campo anterior
- Layout: `[operação: 1][tópico: 2][ID do cliente: 8][tamanho do nome: 1][nome: N][payload: restante]`.
- O cabeçalho fixo tem 12 bytes. O payload ocupa no máximo `2048 - 12 - N` bytes; o encoder trunca o payload que exceder esse limite.
- Em `SEND`, o nome pode ser vazio (tamanho `0`). Mesmo quando preenchido, o servidor ignora esse nome.
- Em `SEND`, o cliente deve preencher seu ID obtido por `IDENTIFY`. O servidor identifica o remetente exclusivamente por esse ID; IDs zero, desconhecidos ou removidos são descartados.
- O endereço UDP de origem não é usado para identificar o remetente. `Client.UdpAddr` continua sendo o destino usado para enviar mensagens aos inscritos.
- Em `FORWARDING`, o servidor preserva o ID do remetente e preenche o nome com `Client.Name` do cadastro correspondente, preservando tópico e payload dentro do limite do datagrama.
- Name vai ser o identificador de topicos no server
- O cliente obtém seu ID pela operação de controle `IDENTIFY` e o envia no cabeçalho de cada mensagem UDP.
