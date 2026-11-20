# Decisões

Este documento registra as decisões de arquitetura, seus motivos e os limites aceitos. Uma decisão não implica que a funcionalidade já esteja concluída: o estado da implementação e as pendências são indicados abaixo.

## Princípios e responsabilidades

- O projeto é um middleware pub/sub: brokers mantêm tópicos e entregam publicações a seus inscritos; clientes consultam metadata para encontrar o broker responsável.
- O cliente publica diretamente no responsável pelo tópico. Não haverá retransmissão de publicações entre brokers. Isso evita um salto adicional e mantém o roteamento explícito no cliente.
- TCP é o transporte de controle porque oferece um fluxo confiável para cadastro, inscrições e respostas. UDP transporta publicações e entregas, preservando os limites de cada datagrama e sem exigir uma conexão por destinatário.
- UDP não oferece confirmação, ordenação ou retransmissão das publicações pela aplicação nesta versão. Escolher disponibilidade não significa garantir entrega sem perdas durante falhas.

## Identidades e índices

- Cada broker tem um ID `uint16` não nulo, informado na criação e único na configuração do cluster. A identidade é configurada para permanecer reconhecível entre execuções; a geração aleatória de metadata distingue cada nova instância.
- Clientes recebem IDs `uint64` e tópicos recebem IDs locais `uint16`. O zero é reservado e a alocação começa em 1.
- `IDSequence` concentra sequência, limite, IDs ocupados e liberação. O mesmo mecanismo atende clientes e tópicos, evitando duas implementações de alocação com regras diferentes.
- Nomes são as chaves de unicidade dos cadastros locais. Índices adicionais por ID apontam para os mesmos objetos para permitir consultas diretas no processamento UDP. Criação e remoção atualizam os índices sob o mesmo mutex. Tópicos recuperados mantêm o nome de apresentação; em caso de colisão, o índice por nome usa o alias estável `nome@LogicalID`. O cliente deve usar o ID lógico quando o nome for ambíguo.
- Um ID numérico local não é uma identidade global: IDs de clientes e tópicos podem coincidir entre brokers. Metadata separa os catálogos por origem.
- Nomes de clientes têm até 255 bytes, contando bytes UTF-8, para caber no tamanho de nome de 1 byte do `FORWARDING`.

## Tamanhos máximos

- TCP: payload de até 256 bytes; prefixo de 4 bytes e headers de 3 bytes ficam fora desse limite (até 263 bytes no total)
- UDP: payload de até 2048 bytes; headers ficam fora desse limite (até 2059 bytes em SEND e 2315 em FORWARDING com nome de 255 bytes)

O limite é do conteúdo da aplicação, e não do pacote completo: acrescentar headers não deve diminuir a capacidade do payload. Constantes próprias separam os limites de TCP e UDP. Controle TCP acima do limite é rejeitado, sem truncar comandos; UDP limita o payload a 2048 bytes por truncamento.

## Enquadramento e formatos de mensagens

- TCP usa um prefixo Big Endian de 4 bytes contendo o tamanho do corpo, sem contar o próprio prefixo. Isso é necessário porque leituras TCP não correspondem necessariamente a mensagens completas.
- Os campos numéricos das mensagens próprias são Big Endian para manter uma representação comum entre participantes.
- UDP não precisa de prefixo de tamanho para separar mensagens: cada leitura recebe um datagrama. O buffer deve comportar o maior datagrama completo, incluindo seus headers.
- `SEND` e `FORWARDING` usam a mesma struct `DataMessage`, mas a operação seleciona o formato codificado. `SEND` transmite operação, tópico, ID do cliente e payload; `FORWARDING` acrescenta tamanho e nome do remetente.
- O nome não é transmitido no `SEND`. O servidor usa o `ClientID` para buscar o cadastro e preenche o nome apenas no `FORWARDING`. Isso reduz o custo das publicações e evita tomar o nome fornecido no datagrama como fonte de identidade.
- O endereço UDP identifica o destino de entrega aos inscritos, não a identidade do remetente. `IDENTIFY` associa nome e ID de cliente; o cadastro do destino UDP é uma etapa de controle separada.

## Concorrência e buffers

- Cada canal UDP possui uma goroutine de leitura para que a espera em um socket não bloqueie os demais.
- Cada cliente possui um buffer e no máximo uma goroutine de envio. Os leitores enfileiram mensagens; não esperam a transmissão de cada destinatário.
- O buffer tem capacidade limitada e descarta a mensagem mais antiga quando cheio. Isso impede crescimento ilimitado de memória, aceitando perda para um destinatário que não acompanha a publicação.
- Um canal Go de capacidade 1 apenas notifica que há trabalho no buffer. Notificações podem ser agrupadas porque as mensagens ficam na fila, e não no canal de notificação.
- Mutexes protegem mapas, índices e filas. Chamadas RPC não mantêm o mutex do servidor bloqueado durante operações de rede.
- `context.Context` coordena cancelamento; `WaitGroup` aguarda o encerramento efetivo das rotinas. O contexto não encerra goroutines à força. Leituras UDP usam prazos curtos para verificar periodicamente o cancelamento.

## Ping

O cliente mantém uma rotina de recuperação que consulta metadata periodicamente. As requisições de cada conexão são serializadas, enquanto uma rotina de leitura distingue respostas de notificações. `PING` também está disponível no controle TCP; o RPC usa seu próprio ping para disponibilidade dos peers.

## Como identificar um cliente UDP

`IDENTIFY` fornece um ID de cliente por broker. `REGISTER_UDP (10)` informa a porta UDP do cliente pela sessão TCP identificada; o servidor combina essa porta com o IP observado na conexão TCP. A identidade da publicação é sempre o `ClientID`, e não o endereço UDP de origem.

## Comunicação entre brokers

- Implementada com `net/rpc` e Gob em um listener TCP dedicado. Cada servidor registra seu próprio serviço `Broker`; a conexão de controle dos clientes não é compartilhada com RPC.
- Os peers são configurados explicitamente por ID e endereço RPC nos dois lados. Não há descoberta automática.
- O catálogo de cada origem usa sua geração e revisão. O catálogo remoto mantém os IDs locais separados por origem. Cada tópico também possui um `LogicalID` aleatório e estável durante transferências, além de `HomeBrokerID` e `OwnerName`. Consultar metadata não insere tópicos remotos nos índices locais; o reconciliador cria uma instância local somente quando este broker é o destino selecionado.
- Alterações locais geram eventos ordenados, com notificações imediatas. Uma reconciliação a cada 5 segundos recupera alterações não entregues; cada chamada tem timeout padrão de 2 segundos.
- O histórico conserva no máximo 4096 eventos ou 4 MiB de eventos codificados. O primeiro limite atingido provoca descarte dos eventos mais antigos. Esses parâmetros são configuráveis.
- Peers com cursor válido recebem deltas. Peers novos, de outra geração, fora do histórico ou com deltas estimados maiores que o estado atual recuperam um snapshot paginado.
- Cada bloco de deltas ou snapshot codificado tem no máximo 16 KiB, sem o envelope RPC. Eventos individuais maiores são fragmentados, sem truncar seus dados.
- O snapshot corresponde a uma revisão fixa, é montado separadamente e instalado de forma atômica. Depois são buscados os deltas posteriores à revisão-base. Se esse histórico expirar, a recuperação reinicia com um snapshot mais recente.
- Uma transferência de snapshot por peer; expiração após 60 segundos sem progresso. A última página é mantida até a expiração para permitir repetição de uma resposta perdida.
- Tudo permanece em memória. Um sobrevivente conserva o diretório dos tópicos conhecidos e permite reconstruir o tópico no destino e no retorno do broker original. As conexões e inscrições são restabelecidas pelo cliente, não replicadas como sockets. Se todos os brokers perderem a memória, não há recuperação durável.
- O núcleo de metadata é separado do RPC. O adaptador TCP dos clientes já usa Protobuf e páginas de até 256 bytes; os deltas entre brokers usam Gob. Manter essa separação permite evoluir cada transporte sem duplicar a regra de responsabilidade dos tópicos.

## Roteamento de publicações

- O cliente é responsável por consultar os metadados e enviar `SEND` diretamente ao broker responsável pelo tópico.
- Brokers não encaminham publicações uns aos outros. O `FORWARDING` UDP é o envio do broker responsável aos seus inscritos, não um mecanismo de roteamento entre brokers.
- Quando a responsabilidade por um tópico mudar, o novo broker e seus endereços precisam aparecer nos metadados para que publishers e subscribers atualizem seu destino.

## Failover e retorno ao broker original

### Disponibilidade em vez de quórum

- **Decisão do usuário:** um sobrevivente pode assumir tópicos sem autorização da maioria. Isso permite continuar atendendo mesmo com apenas um broker acessível.
- **Limite aceito:** durante uma partição, grupos com visões diferentes podem atender o mesmo tópico. A escolha determinística só produz o mesmo destino com a mesma visão de disponibilidade. Não há promessa de dono global exclusivo, consenso forte ou recuperação de todas as publicações.
- Um peer indisponível continua sendo considerado no período de tolerância `FailoverAfter`, padrão `3 * SyncInterval` (15 s na configuração padrão). Motivo: evitar transferências por falhas muito breves.
- O reconciliador executa a cada `FailoverInterval`, padrão `min(SyncInterval, 1 s)`. A detecção depende de chamadas e timeouts de rede; esses tempos não representam uma garantia rígida de recuperação.

### Regra de destino e identidade

- O tópico prefere sempre seu `HomeBrokerID` quando esse broker é considerado disponível. Esta é a decisão de **retorno automático ao original**, escolhida pelo usuário para restaurar a distribuição original após a recuperação.
- Sem o original, usamos rendezvous hashing: para cada broker disponível, calculamos SHA-256 de `LogicalID/ID-do-broker` e escolhemos a maior pontuação, com menor ID como desempate. Motivo: distribuir tópicos e garantir uma escolha independente da ordem dos mapas para uma mesma visão.
- A transferência preserva `LogicalID`, nome, broker original e nome do cliente proprietário. O novo dono aloca um ID local livre. O cliente recebe a nova associação e usa o ID local somente naquele destino.
- O substituto mantém o tópico durante a transição até observar o destino preferido anunciando a mesma identidade lógica. Depois libera sua hospedagem; isso não equivale a excluir o tópico.
- O diretório dos tópicos conhecidos é replicado entre sobreviventes. Assim, um original que reinicie sem memória consegue conhecer e reconstruir seus tópicos antigos.
- Exclusão lógica usa tombstones (registros de exclusão) replicados. Liberar uma hospedagem usa um evento diferente. Motivo: impedir que uma cópia em cache ressuscite um tópico que o proprietário realmente excluiu.

### Cliente, controle e metadata

- `ListenTCP` implementa sessões de controle com `IDENTIFY`, `REGISTER_UDP`, `ADD`, `DEL`, `SUB`, `UNSUB`, `PING` e `GET_META`. `UPD_META` comunica mudanças de rotas.
- Os headers de controle e o prefixo são lidos com `io.ReadFull`; escritas são serializadas para não misturar respostas e notificações. O decoder completo permanece disponível para compatibilidade.
- `GET_META` e `UPD_META` transportam `RouteTable` em páginas `RoutePage` do schema `message/routes.proto`. O codec Protobuf específico é implementado sem dependência de runtime externo e tem testes dos bytes do formato.
- A versão da tabela é um hash do conteúdo ordenado. O cliente pode enviar a versão em `GET_META`; se nada mudou, o servidor responde `unchanged`, evitando reenviar a tabela inteira. `UPD_META` só envia uma tabela quando essa versão muda.
- A versão é um identificador de conteúdo, não uma revisão global ordenável nem prova de consenso. Uma visão pode ficar temporariamente desatualizada durante propagação ou partição.
- O cliente conserva os IDs lógicos desejados, aprende endereços de outros brokers e recupera suas conexões automaticamente. Cada destino tem uma sessão TCP e um ID de cliente próprios; nenhum ID de cliente é reutilizado como identidade global.
- Após reconectar: reconhecer o nome, obter o ID local, registrar a porta UDP e refazer as inscrições. Um cliente com tópicos em vários destinos mantém várias sessões.
- Publicações continuam indo diretamente ao dono anunciado, sem relay entre brokers. Durante indisponibilidade, a chamada retorna erro se não houver rota/conexão; datagramas já enviados não têm confirmação e podem se perder. Não há replay automático, para não introduzir duplicação implícita.
- Desconectar um cliente remove suas inscrições e sua rotina de envio, preservando os tópicos e o nome do proprietário para reconexão. Excluir um tópico exige uma operação explícita do proprietário.

### Estado implementado e validação

Implementados: destino determinístico, tolerância a falhas breves, identidade lógica, diretório replicado, tombstones, recuperação da hospedagem, retorno automático, divulgação de rotas Protobuf, reconexão e restauração de inscrições no cliente. `Server.Run` inicia TCP, UDP e RPC e encerra o conjunto em caso de falha ou cancelamento; `Server.Close` fecha os listeners. O construtor também fecha os sockets UDP já abertos se a criação falhar.

Os testes cobrem escolha independente da ordem, visões divergentes aceitas sem quórum, colisão de nomes, liberação sem exclusão, framing TCP, formato Protobuf e fluxo real com três brokers: publicação, queda, entrega pelo substituto, retorno com ID local remapeado e exclusão sem ressurreição posterior.

## Pendências remanescentes

- Persistir o diretório e o estado necessário para recuperar uma queda de todos os brokers. A recuperação atual depende de pelo menos um sobrevivente ter conhecido o tópico antes da falha.
- Definir compactação/coleta dos tombstones e do diretório conhecido. O histórico de eventos tem limite, mas esses registros de estado são mantidos em memória para evitar ressurreição após desconexões longas.
- Evoluir metadata dos clientes para deltas por cursor. Atualmente os brokers usam deltas/snapshots; clientes recebem tabelas completas paginadas somente quando o conteúdo muda, com resposta condicional quando não muda.
- Definir resolução de publicações e operações conflitantes entre partições, caso se deseje alguma garantia além da convergência eventual da rota. A arquitetura atual aceita donos simultâneos.
- Definir persistência, confirmação ou retransmissão de publicações, caso se deseje entrega durável. O transporte UDP atual é best effort.
- Tornar configuráveis os endereços TCP/UDP dos clientes, ainda limitados a `127.0.0.1` no construtor; RPC já permite endereço anunciado.
- Definir descoberta dinâmica e alterações de membros em execução. A seleção atual pressupõe uma configuração estática de peers conhecida pelos participantes.
- Tratar NAT/redes em que a porta UDP externa difere da registrada pela conexão TCP. O registro atual atende o caso de endereço diretamente acessível.

Não estão planejados relay de publicações entre brokers, migração de sockets TCP existentes nem consenso forte. Esses limites são escolhas de arquitetura, não funcionalidades que a implementação atual promete oferecer.
