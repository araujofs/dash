# Melhorias

## Mensagem de controle

- Os 4 bytes de tamanho devem ser lidos fora, não entram no decode da mensagem (é necessário conhecer esse tamanho antes de ler a mensagem de fato)
- Na declaração de bodySize em encode o 3 que são os headers devem usar a const pra evitar retrabalho caso o tamanho dos headers mude
- Padronizar a lingua usada no codigo pra ingles
- No decode não precisa verificar se o tamanho lido é diferente do tamanho passado, quem precisa garantir isso é o cliente, ele vai ler os proximos X bytes com base nos 4 bytes de tamanho, nem mais, nem menos, nao tem como vir maior, ele vai ler no maximo aquilo (pode até ser feito, para evitar mal uso do cliente pela gente, mas deve vir certo já de fora)

## Mensagem de dados

- **Bug no `Encode` de `DataMessage`**: na linha 41 de `message/data_message.go`, está `copy(bytes[:3], []byte(d.Payload))`. Isso sobrescreve os 3 primeiros bytes que guardam a Operação e o Tópico. Deve ser corrigido para `copy(bytes[3:], []byte(d.Payload))`.
- **Tamanho do slice no `Encode`**: atualmente aloca fixo `make([]byte, consts.MAX_DATA_MESSAGE_SIZE)` (2048B). Isso preenche o restante com bytes nulos (`\x00`), o que faz com que no `DecodeDataMessage` o `string(bytes[3:])` venha cheio de zeros no final. O ideal seria alocar apenas o tamanho real (`consts.HEADERS_SIZE + len(d.Payload)`) ou fatiar até o tamanho útil.

