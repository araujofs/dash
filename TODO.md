# Planejamento e Divisão do Projeto (dash)

> **Equipe:** Davi Leite e Arthur Felipe Araujo da Silva  
> **Papel do Davi:** Cliente, roteamento, distribuição de tópicos e descoberta.  
> **Papel do Arthur:** Broker, processamento de mensagens, buffers e recuperação de falhas.

---

## 📌 Etapa 1 — Comunicação e Protocolo (Em Andamento)
**Meta da etapa:** Clientes e brokers conseguem trocar mensagens de controle (TCP) e dados (UDP).

### Tarefas do Davi (Cliente)
- [x] **Comunicação TCP base do cliente**
  - [x] Struct preparada para múltiplos brokers de bootstrap (`[]Broker`)
  - [x] Conexão com failover simples no bootstrap (`Connect()`)
  - [x] Envio de mensagens de controle protegido por mutex e validando limite de 256B (`SendControl()`)
  - [x] Encerramento de conexão TCP (`Close()`)
  - [x] Leitura de mensagens de controle via `io.ReadFull`, validação de tamanho de corpo e decode (`ReadControl()`)
- [x] **Emissão das operações de controle**
  - [x] Envio e tratamento de resposta de `SUB` (0) com nome do cliente no payload
  - [x] Envio e tratamento de resposta de `UNSUB` (1)
  - [x] Envio e tratamento de resposta de `ADD` (2) para criação de tópico
  - [x] Envio e tratamento de resposta de `DEL` (3) para exclusão de tópico
  - [ ] Envio de solicitação de metadados `GET_META` (5) (depende de Protobuf com Arthur)
  - [x] Envio de `PING` (6) / verificação de conexão
- [ ] **Comunicação UDP do lado do cliente**
  - [ ] Inicialização da conexão UDP (`udpConn *net.UDPConn`) no cliente
  - [ ] Funções base de envio de datagramas de dados
  - [ ] Funções base de escuta de datagramas de dados

---

## 📌 Etapa 2 — Tópicos e Inscrições
**Meta da etapa:** Clientes conseguem criar/deletar tópicos e realizar inscrições/desinscrições.

### Tarefas do Davi (Cliente)
- [x] Implementar métodos de alto nível para solicitação de criação (`CreateTopic`) e exclusão (`DeleteTopic`) de tópicos
- [x] Implementar métodos de alto nível para solicitação de inscrição (`Subscribe`) e desinscrição (`Unsubscribe`)
- [ ] Implementar consulta das informações dos tópicos disponíveis (`GetTopics` / consulta ao cache de metadados)
- [ ] Testes unitários das operações de tópicos e inscrições do cliente

---

## 📌 Etapa 3 — Publicação e Distribuição de Mensagens
**Meta da etapa:** Um cliente consegue publicar mensagens em um tópico via UDP e consumidores inscritos conseguem recebê-las.

### Tarefas do Davi (Cliente)
- [ ] Implementar a publicação de mensagens via UDP pelo cliente (`Publish(topic, payload)`) com operação `SEND` (7)
- [ ] Implementar o direcionamento da publicação diretamente ao broker responsável pelo tópico (baseado no cache)
- [ ] Implementar loop de recebimento de mensagens (`FORWARDING` - 8) pelos consumidores inscritos via UDP
- [ ] Testes de publicação, recepção e integridade de mensagens de dados

---

## 📌 Etapa 4 — Cluster, Descoberta e Balanceamento
**Meta da etapa:** Os 3 brokers funcionam como um cluster, com tópicos distribuídos e clientes capazes de identificar seus respectivos brokers.

### Tarefas do Davi (Cliente)
- [ ] Implementar a descoberta dos brokers a partir dos endereços de bootstrap
- [ ] Implementar o cache de metadados no cliente (`Topics map[uint16]uint16`, `Brokers map[uint16]Broker`)
- [ ] Implementar a lógica de distribuição e balanceamento dos tópicos entre os brokers
- [ ] Implementar o roteamento das operações do cliente com base nos metadados do cluster
- [ ] Testes de descoberta, cache e algoritmo de balanceamento

---

## 📌 Etapa 5 — Detecção e Recuperação de Falhas
**Meta da etapa:** Quando um broker fica indisponível, o cluster redistribui responsabilidades e clientes continuam operando.

### Tarefas do Davi (Cliente)
- [ ] Implementar a atualização dos metadados do cliente ao receber `UPD_META` (4) após mudanças na topologia
- [ ] Implementar o redirecionamento automático das publicações para os novos brokers responsáveis
- [ ] Implementar o redirecionamento/reinscrição das assinaturas para os novos brokers responsáveis
- [ ] Testes de resiliência e failover no cliente

---

## 📌 Testes e Validação Integrada (Davi & Arthur)
- [ ] Teste de integração ponta a ponta (Cliente $\leftrightarrow$ Broker via TCP e UDP)
- [ ] Teste de cluster com 3 brokers ativos
- [ ] Teste de continuidade do serviço após derrubar um dos brokers (recuperação de falhas)
