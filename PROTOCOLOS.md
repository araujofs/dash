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
- REGISTER_UDP (10): registrar a porta UDP de entrega pela sessão TCP identificada

### Reconhecimento do cliente (IDENTIFY)

- O cliente envia uma mensagem de controle com operação `IDENTIFY (9)`, tópico `0` e payload contendo apenas seu nome em UTF-8, de 1 a 255 bytes. O servidor conhece e armazena esse nome após o reconhecimento; o cliente não precisa enviá-lo nas publicações UDP.
- O nome deve ser único entre os clientes cadastrados nesse servidor. Nome vazio, acima do limite ou já cadastrado resulta em falha.
- O servidor cadastra o cliente e atribui um ID `uint64` diferente de zero, usando sua sequência de clientes.
- A resposta também usa operação `IDENTIFY` e tópico `0`, mantendo o prefixo de tamanho das mensagens de controle.
- Payload de sucesso: `[status: 1 byte = 0][ID do cliente: 8 bytes, Big Endian]`.
- Payload de falha: `[status: 1 byte = 1][descrição do erro: UTF-8]`, sem ID.
- O ID identifica o cadastro enquanto o cliente permanecer conectado e pode ser reutilizado após sua remoção. Seu escopo é o servidor que o atribuiu.

### Decisões

- As mensagens (headers + payload) devem ser prefixadas por 4 bytes. O prefixo informa o tamanho do corpo (headers + payload), sem contar os próprios 4 bytes.
- O payload de cada mensagem TCP tem no máximo 256 bytes. Os 4 bytes de prefixo e os 3 bytes de headers ficam fora desse limite, permitindo até 263 bytes no total. O tamanho declarado no prefixo deve ficar entre 3 e 259 bytes.
- Mensagens acima do limite devem ser rejeitadas, sem truncar o payload.
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
- `SUB` identifica o cliente pela sessão estabelecida em `IDENTIFY` e usa o ID local do tópico no header; não é necessário repetir o nome no payload.
- GET_META deve retornar e UPD_META deve enviar os seguintes dados:
  - Brokers ativos
    - Tópicos
      - Nome
      - Identificador
- O limite de 256 bytes de payload TCP vale tanto para requisições quanto para respostas.
- Cliente deve confiar no tamanho das respostas do servidor

## Dados

### Operações

- SEND (7): publicação de mensagem do publisher
- FORWARDING (8): encaminhamento de mensagem para consumers

### Decisões

- O payload UDP pode ter até 2048 bytes, tanto em `SEND` quanto em `FORWARDING`; os headers ficam fora desse limite. O buffer de recepção deve comportar até 2315 bytes, incluindo o maior cabeçalho possível.
- Os headers comuns a `SEND` e `FORWARDING` são os seguintes (em ordem):
  - Operação - 1 byte
  - Tópico - 2 bytes
  - ID do cliente remetente - 8 bytes (`uint64`, Big Endian)
- Somente `FORWARDING` acrescenta os seguintes campos após o ID:
  - Tamanho do nome do usuário - 1 byte, de 0 a 255, contando bytes UTF-8
  - Nome do usuário - quantidade de bytes indicada pelo campo anterior
- Nomes cadastrados têm no máximo 255 bytes, conforme a capacidade do campo de tamanho em `FORWARDING`, e cabem no payload de `IDENTIFY`.
- Layout de `SEND`: `[operação: 1][tópico: 2][ID do cliente: 8][payload: restante]`. Não há campo de nome nem tamanho de nome; o payload começa imediatamente após o ID. O cabeçalho tem 11 bytes e a mensagem completa pode ter até 2059 bytes.
- Layout de `FORWARDING`: `[operação: 1][tópico: 2][ID do cliente: 8][tamanho do nome: 1][nome: N][payload: restante]`. A mensagem completa pode ter até `2048 + 12 + N` bytes, chegando a 2315 bytes com um nome de 255 bytes.
- O encoder e o decoder truncam apenas o payload que ultrapassar 2048 bytes. Acrescentar o nome no encaminhamento não reduz o espaço disponível para o payload.
- Em `SEND`, o cliente deve preencher seu ID obtido por `IDENTIFY`. O servidor identifica o remetente exclusivamente por esse ID; IDs zero, desconhecidos ou removidos são descartados.
- O cliente consulta os metadados para localizar o broker responsável pelo tópico e envia `SEND` diretamente a ele. Brokers não roteiam nem encaminham publicações entre si.
- O endereço UDP de origem não é usado para identificar o remetente. `Client.UdpAddr` continua sendo o destino usado para enviar mensagens aos inscritos.
- Em `FORWARDING`, o servidor preserva o ID do remetente e preenche o nome com `Client.Name` do cadastro correspondente, preservando tópico e payload dentro do limite do datagrama.
- Name vai ser o identificador de topicos no server
- O cliente obtém seu ID pela operação de controle `IDENTIFY` e o envia no cabeçalho de cada mensagem UDP.

## RPC entre brokers

### Componentes e identidade

O RPC utiliza `net/rpc` com Gob em um listener TCP próprio. Não usa o prefixo nem o limite de 256 bytes das mensagens de controle dos clientes. O adaptador dos clientes usa Protobuf conforme `message/routes.proto`, paginado em payloads de até 256 bytes.

| Componente | Responsabilidade |
| --- | --- |
| `MetadataStore` | Catálogo autoritativo local, revisão e histórico limitado de eventos |
| `MetadataReplica` | Catálogo de uma origem remota, cursor aplicado e remontagem de deltas |
| `BrokerRPC` | Métodos RPC de ping, transferência de alterações e snapshots |
| Sincronizador por peer | Reconciliação, notificações imediatas, confirmação de eventos e recuperação |

Cada nova instância de `MetadataStore` recebe uma geração aleatória. Cada alteração incrementa uma revisão `uint64`. O cursor é o par `(Generation, Revision)` e identifica a última revisão completamente aplicada. A geração permanece a mesma ao reiniciar apenas `ListenRPC` no mesmo objeto `Server`; um novo servidor recebe outra geração.

Metadados incluem ID do broker, endereços RPC/TCP/UDP, tópicos hospedados, diretório de tópicos conhecidos e registros de exclusão lógica. Cada tópico inclui ID local, nome, `LogicalID`, `HomeBrokerID` e `OwnerName`. Os IDs locais continuam separados por broker; a identidade lógica permanece estável durante transferências. Sockets, inscrições e publicações não são replicados: o cliente restabelece as sessões e inscrições necessárias.

Os peers precisam estar configurados reciprocamente por ID e endereço RPC. Requisições de IDs não configurados são rejeitadas; o sincronizador verifica se o ID e a geração das respostas correspondem à origem esperada.

### Métodos do serviço `Broker`

Todos os métodos seguem a assinatura padrão de `net/rpc`: argumento exportado, resposta por ponteiro e retorno `error`.

| Método | Entrada | Resposta |
| --- | --- | --- |
| `Ping` | `PeerRequest`: ID do chamador | `BrokerIdentity`: ID, cursor local e cursor já recebido daquele chamador |
| `GetChanges` | `ChangesRequest`: ID, cursor, revisão final do lote e posição de fragmento | `MetadataChunk`: geração, revisões anterior/final, posição, dados, término, existência de mais eventos e necessidade de snapshot |
| `ApplyChanges` | `ApplyChangesRequest`: ID e fragmento/lote | `MetadataAck`: cursor confirmado, posição de remontagem ou necessidade de recuperação |
| `BeginSnapshot` | `PeerRequest`: ID do chamador | `SnapshotInfo`: identificador de transferência, cursor-base e tamanho codificado |
| `ReadSnapshot` | `SnapshotRequest`: ID, identificador de transferência e posição | `SnapshotChunk`: dados, posição, término ou expiração |

Os erros de aplicação de `net/rpc` chegam ao chamador como erros de texto. Condições normais de recuperação, como necessidade de snapshot e expiração, são campos das respostas, e não erros de rede.

### Atualizações incrementais

Os eventos são `BrokerUpdated`, `TopicUpdated`, `TopicRemoved`, `TopicReleased`, `TopicKnown` e `TopicDeleted`. `TopicReleased` remove somente a hospedagem; `TopicDeleted` propaga uma exclusão lógica permanente. `TopicKnown` registra uma identidade já aprendida para permitir recuperação após reinício do original. `AddTopic` e `RemoveTopic` atualizam os índices locais e registram o evento sob o mesmo mutex do servidor. Operações rejeitadas não geram eventos. O histórico é inicializado com os dados já presentes quando o componente de metadata é criado, permitindo criar tópicos antes de iniciar RPC.

Após uma alteração, cada sincronizador recebe uma notificação não bloqueante. Cada peer possui uma rotina; a cada rodada, ela verifica a identidade, recupera as alterações remotas e envia as alterações locais ainda não confirmadas. Se o destinatário precisar de snapshot, `ApplyChanges` o sinaliza para que seu sincronizador busque a base no remetente. Não há chamada de rede mantendo o mutex do servidor.

Cada lote contém uma lista Gob de `MetadataEvent`. Os campos `After` e `Through` fixam seu intervalo de revisões. `Offset` identifica a posição no conteúdo codificado durante a fragmentação. O cursor público avança somente após a remontagem e aplicação integral do lote.

`MetadataChunk` e `SnapshotChunk`, codificados em Gob, têm no máximo 16 KiB cada. Os envelopes adicionais de chamada e de identificação RPC não entram nesse orçamento. Um evento maior que o limite atravessa várias chamadas, preservando seus dados.

Eventos já confirmados não são reaplicados; fragmentos repetidos não duplicam os bytes remontados. Uma lacuna exige recuperação a partir do cursor confirmado. Eventos de hospedagem remota não são inseridos como tópicos hospedados localmente. O diretório conhecido e os tombstones são incorporados uma única vez por identidade lógica e geram eventos próprios, permitindo que outro sobrevivente ensine esses dados ao broker original após reinício. A deduplicação desses fatos evita ciclos de geração contínua de eventos.

### Escolha entre deltas e snapshot

O histórico mantém, por padrão, até 4096 eventos ou 4 MiB de registros codificados. O catálogo atual é preservado mesmo quando eventos antigos são descartados. A estimativa do tamanho do estado é atualizada por registro, sem gerar um snapshot completo a cada mudança.

`GetChanges` solicita snapshot quando não há cursor compatível, a revisão necessária já foi descartada ou os bytes estimados de eventos pendentes são iguais ou maiores que os do estado atual. A fragmentação já iniciada mantém seu intervalo fixo; se os eventos correspondentes expirarem, também exige recuperação.

### Snapshot e recuperação

1. `BeginSnapshot` copia um catálogo consistente e o cursor da revisão `R`, liberando o mutex antes de codificá-lo.
2. `ReadSnapshot` entrega páginas de até 16 KiB. O receptor acumula essas páginas em uma área temporária.
3. Após validar tamanho, origem e geração, o receptor instala a base inteira de forma atômica. Isso remove tópicos antigos ausentes no snapshot.
4. O receptor consulta os deltas posteriores a `R`. Alterações ocorridas durante a transferência não são perdidas enquanto esse intervalo estiver no histórico.
5. Se o histórico necessário expirar, a sessão é invalidada e começa uma transferência atualizada. São feitas até três novas tentativas imediatas por rodada; depois a próxima notificação ou reconciliação periódica tenta novamente.

Existe no máximo uma sessão por peer. Ela expira após 60 segundos sem leitura e é removida por limpeza periódica. A última página permanece disponível até expirar, permitindo repetir uma leitura cuja resposta tenha sido perdida.

Gerações substituídas são lembradas pelo receptor para rejeitar respostas atrasadas. Um snapshot antigo da mesma geração também não substitui um catálogo que já tenha uma revisão maior. A substituição afeta somente a origem correspondente.

### Disponibilidade e configuração

Padrões: reconciliação a cada 5 segundos, timeout por chamada de 2 segundos, histórico de 4096 eventos/4 MiB e expiração de snapshot de 60 segundos. Esses limites podem ser configurados em `RPCConfig`. O limite de bloco é `MetadataBatchBytes` (16 KiB).

Falhas preservam o último catálogo e o cursor, marcando o peer indisponível e não sincronizado. Rodadas bem-sucedidas restauram sua disponibilidade. O cancelamento de `ListenRPC` fecha o listener e as conexões aceitas, cancela as chamadas de saída e aguarda as rotinas terminarem.

`RPCAddress()` consulta o endereço anunciado em execução e retorna vazio após encerramento. `ClusterMetadata()` retorna cópias do estado local e dos peers, ordenadas por ID, com disponibilidade, sincronização e instante do último sucesso. `Available` refere-se à observação local do serviço RPC; não constitui consenso de disponibilidade do cluster.

### Metadata dos clientes e roteamento

O schema `message/routes.proto` define `TopicRoute`, `BrokerEndpoint`, `RouteTable` e `RoutePage`. O codec em Go implementa esse formato Protobuf sem dependência de runtime externo. Tipos numéricos dentro do Protobuf usam a codificação definida pelo Protobuf; a regra Big Endian se aplica aos headers próprios e ao prefixo TCP, não aos varints internos do Protobuf.

- `TopicRoute`: identidade lógica, nome, broker atual, ID local atual, endereços TCP/UDP, nome do proprietário e broker original.
- `RouteTable`: versão, lista de rotas e lista de endpoints dos brokers conhecidos com sua disponibilidade observada.
- `RoutePage`: versão, posição em bytes, fragmento de dados, indicador de última página e indicador opcional `unchanged`.
- A versão é um hash do conteúdo ordenado da tabela. É um identificador de conteúdo; não é uma revisão global monotônica nem uma prova de consenso.
- O cliente monta a tabela separadamente e só a publica quando todas as páginas da mesma versão foram recebidas. Isso evita expor uma rota parcialmente transferida.

`GET_META` aceita no payload a versão já conhecida, ou payload vazio para consulta incondicional. A resposta contém uma ou mais mensagens `GET_META`, cada uma com um `RoutePage` de até 256 bytes. Se a versão coincide, retorna uma única página com `done=true`, `unchanged=true` e dados vazios. O cliente mantém a tabela anterior nesse caso.

Depois de `IDENTIFY`, o servidor verifica mudanças de rota a cada 100 ms. Quando o conteúdo muda, envia `UPD_META` paginado no mesmo formato. O cliente confirma o término com uma mensagem `UPD_META` de payload `[0]`; o servidor não responde novamente a essa confirmação. As escritas de cada tabela são serializadas com as respostas de controle para não intercalar páginas de transferências diferentes.

Nesta versão, clientes recebem tabelas completas quando há mudança e respostas condicionais quando não há. Deltas por cursor para clientes continuam uma evolução possível: o núcleo de metadata pode ser reaproveitado, mas o formato atual de rotas não replica o journal RPC para cada sessão.

### Demais operações de controle implementadas

Todas as operações exigem uma sessão identificada, exceto `IDENTIFY`. Erros de operações usuais retornam `[status=1][mensagem de erro]`, respeitando 256 bytes de payload.

| Operação | Requisição | Resposta de sucesso |
| --- | --- | --- |
| `IDENTIFY` | Tópico 0; nome UTF-8 no payload | `[0][ClientID uint64 Big Endian]` |
| `REGISTER_UDP` | Tópico 0; porta UDP em 2 bytes Big Endian | `[0]` |
| `ADD` | Tópico 0; nome do tópico no payload | `[0][ID local uint16 Big Endian][LogicalID UTF-8]` |
| `DEL` | ID local no header; payload vazio | `[0]` |
| `SUB` / `UNSUB` | ID local no header; sessão determina o cliente | `[0]` |
| `PING` | Payload vazio | `[0]` |

`REGISTER_UDP` combina a porta enviada com o IP observado na conexão TCP. Ele não determina a identidade de `SEND`, que continua sendo o ID do header. Redes com tradução de porta UDP exigem uma extensão futura de registro.

`SUB` e `UNSUB` são idempotentes no atendimento TCP para permitir repetição após reconexão. Os métodos Go de inscrição por nome continuam retornando erro para inscrições duplicadas ou remoções inexistentes. `DEL` verifica o nome do proprietário preservado no tópico. Desconectar um cliente retira suas inscrições e encerra seu worker, preservando os tópicos para reconexão.

### Failover e retorno automático

1. Os brokers conservam as identidades lógicas que aprenderam e os registros de exclusão. A queda de um broker não apaga esse diretório.
2. Um peer sem sucesso recente é considerado indisponível após `FailoverAfter`, cujo padrão é `3 * SyncInterval`. O reconciliador roda a cada `FailoverInterval`, padrão `min(SyncInterval, 1 s)`.
3. Para cada tópico não excluído, o destino é seu `HomeBrokerID` quando disponível. Caso contrário, vence a maior pontuação SHA-256 de `LogicalID/ID-do-broker` entre os disponíveis; menor ID desempata pontuações iguais.
4. O vencedor cria a hospedagem local, aloca um ID livre e preserva nome, identidade lógica, broker original e proprietário. A associação aparece no metadata hospedado e na tabela de rotas.
5. O cliente aprende a nova rota, abre a sessão do destino, executa `IDENTIFY` e `REGISTER_UDP` e refaz as inscrições desejadas. Usa um ID de cliente diferente por sessão/broker.
6. Quando o original retorna, passa a ser novamente o destino preferido. Ele reconstrói os tópicos a partir do diretório dos sobreviventes. O substituto libera sua hospedagem após observar a hospedagem no destino preferido; liberar não gera exclusão lógica.
7. Os clientes acompanham a rota de volta e restabelecem as inscrições. Publicações continuam indo diretamente ao dono anunciado, sem relay entre brokers.

A escolha usa a visão local de disponibilidade e **não exige quórum**. A prioridade é disponibilidade, conforme decisão do usuário: partições com visões diferentes podem produzir donos simultâneos. Após a conectividade retornar, a preferência determinística reconcilia a rota. Isso não resolve conflitos de mensagens ou garante entrega sem perdas.

Se duas identidades de origem possuírem o mesmo nome, elas permanecem distintas. A hospedagem recuperada usa `nome@LogicalID` como alias no índice local caso necessário. O nome apresentado no metadata não muda. Clientes devem usar `LogicalID` para evitar ambiguidade.

Tombstones se propagam para que uma cópia antiga não ressuscite um tópico excluído. Eles e o diretório conhecido permanecem em memória; sua coleta e persistência ainda precisam de política própria. Se nenhum sobrevivente tiver conhecido o tópico, ou todos perderem a memória, essa versão não consegue recuperá-lo.

### API de execução e cliente

`Server.Run(ctx, RPCConfig)` inicia TCP de clientes, UDP e RPC; uma falha de serviço ou cancelamento encerra o conjunto. Também é possível executar `ListenTCP`, `ListenUDP` e `ListenRPC` separadamente. `Server.Close` fecha os listeners e solicita o encerramento RPC; os loops aguardam as rotinas que iniciaram antes de retornar.

`client.New(nome, seedsTCP)` cria o cliente. `Connect(ctx)` inicia reconhecimento, consulta de rotas e monitor de recuperação. `Subscribe` guarda a identidade lógica desejada; `Recover` atualiza rotas e restaura inscrições automaticamente; `SubscriptionReady` permite consultar se a inscrição atual já foi restabelecida. `Publish` envia diretamente ao endereço UDP da rota, usando o ID da sessão daquele broker. `Receive` lê `FORWARDING`, e `Close` encerra sessões e socket UDP.

As rotas novas são aplicadas antes de refazer as inscrições, portanto conhecer a rota não implica que a inscrição já esteja pronta; use `SubscriptionReady` quando essa distinção importar. Falhas e ausência de rota podem resultar em erro de publicação. Não há fila durável nem retransmissão automática de datagramas enviados durante a falha.

O failover recupera a disponibilidade de tópicos e reconecta clientes vivos; não migra sockets existentes nem persiste estado após perda de todos os brokers. Descoberta dinâmica e consenso forte ficam fora da arquitetura atual. Veja os motivos e pendências atualizados em `DECISOES.md`.
