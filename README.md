# dash
Middleware pub/sub simples.

## RPC entre brokers

O servidor oferece ping e sincronização de metadata com `net/rpc`, em uma porta separada do controle dos clientes. Alterações usam deltas; brokers novos ou muito atrasados recebem um snapshot paginado e depois os deltas posteriores. O catálogo remoto é separado dos tópicos hospedados. O reconciliador cria hospedagens locais apenas quando o broker é selecionado para recuperar um tópico.

Exemplo para o broker 1, considerando outro broker com ID 2 em `127.0.0.1:9002`:

```go
func runBroker(ctx context.Context) error {
    broker, err := server.NewServer(1, 8001)
    if err != nil {
        return err
    }
    return broker.Run(ctx, server.RPCConfig{
        ListenAddr: "127.0.0.1:9001",
        Peers: []server.PeerConfig{
            {ID: 2, RPCAddr: "127.0.0.1:9002"},
        },
    })
}
```

O broker 2 deve anunciar `127.0.0.1:9002` e configurar o peer 1 com `127.0.0.1:9001`. A lista é estática e recíproca. `Run` inicia os serviços TCP, UDP e RPC com encerramento conjunto. As funções `ListenTCP`, `ListenUDP` e `ListenRPC` também podem ser usadas separadamente.

- `ListenAddr` aceita porta `0`; `RPCAddress()` retorna o endereço efetivo após iniciar a escuta.
- Para escutar em `0.0.0.0` ou `::`, configure `AdvertiseAddr` com um endereço acessível. Se sua porta for `0`, será substituída pela porta efetivamente aberta.
- `ClusterMetadata()` retorna cópias dos metadados locais/remotos e seus estados de disponibilidade e sincronização.
- `SyncInterval`, `Timeout`, `HistoryEvents`, `HistoryBytes` e `SnapshotTTL` configuram os padrões de 5 s, 2 s, 4096 eventos, 4 MiB e 60 s. `FailoverAfter` é por padrão `3 * SyncInterval`, e `FailoverInterval` é `min(SyncInterval, 1 s)`.
- Cada bloco codificado de deltas ou snapshot tem até 16 KiB. Eventos maiores são fragmentados. O RPC não está sujeito ao payload TCP de 256 bytes dos clientes.
- O cancelamento do contexto encerra listener, conexões RPC e goroutines. O histórico permanece no objeto `Server`; criar outro objeto inicia uma nova geração.

O modelo é eventual e mantido em memória. Ele sincroniza metadata, sem replicar clientes, inscrições ou publicações. Veja os componentes, métodos e fluxos de recuperação em [PROTOCOLOS.md](PROTOCOLOS.md#rpc-entre-brokers) e as pendências em [DECISOES.md](DECISOES.md#pendências-remanescentes).

O roteamento é responsabilidade do cliente: ele consulta os metadados e publica diretamente no broker responsável pelo tópico. Brokers não encaminham publicações entre si. O failover seleciona o substituto deterministicamente e o cliente restabelece suas sessões e inscrições; quando o original volta, os tópicos retornam a ele. A política prioriza disponibilidade sem quórum e aceita donos simultâneos durante partições, conforme [DECISOES.md](DECISOES.md#failover-e-retorno-ao-broker-original).

## Cliente com recuperação automática

```go
c := client.New("alice", []string{"127.0.0.1:8001", "127.0.0.1:8002"})
if err := c.Connect(ctx); err != nil {
    return err
}
defer c.Close()

logicalID, err := c.CreateTopic(ctx, "orders")
if err != nil {
    return err
}
if err := c.Subscribe(ctx, logicalID); err != nil {
    return err
}
if err := c.Publish(ctx, logicalID, "conteúdo"); err != nil {
    return err
}
msg, err := c.Receive(ctx)
```

Use um contexto que permaneça válido enquanto o cliente estiver em execução. `RetryInterval` (padrão 250 ms) e `Timeout` (2 s) podem ser configurados antes de `Connect`. `NewClient(nome, client.Broker{EnderecoTCP: ...})` e `Conectar` são alternativas para inicialização legada.

- O cliente aprende os endereços dos brokers pela tabela de rotas e mantém uma sessão/ID de cliente por destino.
- O ID lógico permanece igual após transferência; ID local e endereços podem mudar. `Resolve(logicalID)` retorna a rota atual e `Routes()` retorna cópias das rotas conhecidas. Nomes ambíguos exigem uso do ID lógico.
- `Subscribe` mantém a intenção de inscrição durante falhas. O monitor chama `Recover` automaticamente; `SubscriptionReady(logicalID)` indica se a inscrição da rota atual já foi restabelecida.
- `GET_META` e `UPD_META` entregam tabelas Protobuf paginadas em até 256 bytes de payload. Consultas com versão conhecida podem responder apenas `unchanged`.
- Na ausência do original, vence a maior pontuação SHA-256 de `LogicalID/ID-do-broker` entre os disponíveis. Quando o original volta, ele é novamente preferido; a liberação no substituto espera observar a hospedagem no destino preferido.
- O diretório conhecido e os tombstones (exclusões lógicas) são mantidos nos sobreviventes. Isso evita perder a identidade do tópico ou ressuscitar uma exclusão ao recuperar uma cópia antiga.
- Conexões TCP são recriadas; sockets não são migrados. O cliente refaz reconhecimento, registro UDP e inscrições e publica diretamente no destino atual.
- Não há replay automático de publicações durante falhas. UDP continua best effort, e a escolha sem quórum não garante um único dono em partições.

A recuperação depende de um sobrevivente ter conhecido o tópico; não substitui persistência após perda de todo o cluster. As razões dessas escolhas e as pendências restantes estão em [DECISOES.md](DECISOES.md).

## Alterações desde o commit `999578f`

Referência: `feat: implementa encaminhamento UDP com buffers por cliente`.
As alterações abaixo foram feitas depois desse commit e descrevem o estado atual da implementação.

### Limites de payload

Os limites de TCP e UDP agora se aplicam somente ao payload. Prefixos e headers ficam fora dessa conta.

| Mensagem | Payload máximo | Prefixo e headers | Tamanho máximo completo |
| --- | --- | --- | --- |
| Controle TCP | 256 bytes | 4 bytes de prefixo + 3 bytes de headers | 263 bytes |
| UDP `SEND` | 2048 bytes | 11 bytes | 2059 bytes |
| UDP `FORWARDING` | 2048 bytes | 12 bytes + nome do remetente | 2315 bytes, com nome de 255 bytes |

Os tamanhos estão centralizados em `consts/consts.go`. `MAX_DATA_MESSAGE_SIZE` representa o maior datagrama possível, incluindo os headers de `FORWARDING`, e dimensiona o buffer de recepção UDP.

### Mensagens de controle TCP

- O encoder rejeita payloads acima de 256 bytes, sem truncamento.
- O decoder rejeita mensagens completas acima de 263 bytes, prefixos que declarem corpos acima de 259 bytes e divergências entre o tamanho declarado e o recebido.
- O prefixo de 4 bytes continua informando o tamanho do corpo: headers de 3 bytes mais payload, sem incluir o próprio prefixo.
- Os tamanhos do prefixo e dos headers passaram a usar constantes próprias, separadas dos headers UDP.

`ControlMessage.Encode()` passou de `[]byte` para `([]byte, error)`. As chamadas precisam tratar o erro:

```go
request := message.NewControlMessage(message.IDENTIFY, 0, []byte("alice"))
packet, err := request.Encode()
if err != nil {
    return err
}
// packet pode ser enviado pela conexão TCP.
```

`ReadControlMessage` já lê prefixo e corpo com `io.ReadFull` e valida o tamanho antes de alocar o corpo. O decoder do frame completo permanece disponível por compatibilidade. O atendimento TCP, `IDENTIFY` e os demais handlers descritos em [PROTOCOLOS.md](PROTOCOLOS.md) estão implementados.

### Formatos distintos para `SEND` e `FORWARDING`

As operações continuam usando `DataMessage`, mas `Operation` determina quais campos são codificados e decodificados.

```text
SEND:
[operação: 1][tópico: 2][ClientID: 8][payload: até 2048]

FORWARDING:
[operação: 1][tópico: 2][ClientID: 8][tamanho do nome: 1][nome: N][payload: até 2048]
```

- `SEND` deixou de transmitir tanto o nome quanto seu campo de tamanho. Mesmo que `DataMessage.Name` esteja preenchido em memória, o encoder não o inclui em `SEND`.
- `FORWARDING` continua transmitindo o nome. O servidor o obtém do cadastro do remetente consultado por `ClientID` e preenche `Name` antes do envio.
- O nome cadastrado continua limitado a 255 bytes, compatível com o campo de tamanho de 1 byte do encaminhamento.
- O encoder e o decoder UDP truncam somente o payload que exceder 2048 bytes. Acrescentar o nome em `FORWARDING` preserva integralmente um payload válido de 2048 bytes.
- Mensagens com cabeçalho incompleto, incluindo tamanho ou conteúdo do nome em `FORWARDING`, são rejeitadas pelo decoder.

O construtor deixou de receber o nome. A assinatura atual é:

```go
func NewMessage(operation Operation, topic uint16, clientID uint64, payload string) *DataMessage
```

Exemplo de publicação:

```go
msg := message.NewMessage(message.SEND, topicID, clientID, "conteúdo")
packet := msg.Encode()
```

### Documentação e validação

[PROTOCOLOS.md](PROTOCOLOS.md) e [DECISOES.md](DECISOES.md) foram alinhados aos limites e formatos atuais. A pendência de substituir o tamanho literal dos headers de controle por constantes foi removida de [MELHORIAS.md](MELHORIAS.md).

Os testes foram ampliados para cobrir:

- Payloads TCP de até 256 bytes, rejeição de 257 bytes e validação do prefixo.
- Codificação e decodificação dos formatos específicos de `SEND` e `FORWARDING`, incluindo a ordem dos bytes dos headers.
- Omissão do nome em `SEND`, nomes UTF-8 e rejeição de headers incompletos em `FORWARDING`.
- Payloads UDP de 2048 bytes e truncamento do excesso, sem descontar os headers.
- Limite de 255 bytes para nomes cadastrados.
- Encaminhamento real de um `SEND` de 2059 bytes para um `FORWARDING` de 2315 bytes, preservando todo o payload e acrescentando o nome cadastrado.

A implementação foi validada com `go test -race ./...` após essas alterações.
