# Melhorias

## Mensagem de controle

- `ReadControlMessage` já lê o prefixo e o corpo com `io.ReadFull`; decidir futuramente se o decoder público deve aceitar somente o corpo ou manter a compatibilidade com o frame completo.
- Padronizar a lingua usada no codigo pra ingles
- No decode não precisa verificar se o tamanho lido é diferente do tamanho passado, quem precisa garantir isso é o cliente, ele vai ler os proximos X bytes com base nos 4 bytes de tamanho, nem mais, nem menos, nao tem como vir maior, ele vai ler no maximo aquilo (pode até ser feito, para evitar mal uso do cliente pela gente, mas deve vir certo já de fora)
