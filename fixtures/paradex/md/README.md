# Paradex SBE market-data frames

Hand-built, not recorded: each file is one binary frame written as whitespace-separated hex
bytes, one field per line, `#` to the end of a line a comment. No account, order or fill frame
and no account value is here (0009).

Layouts follow Paradex's published schema, `paradex_1_0.xml` in
[tradeparadex/paradex-py](https://github.com/tradeparadex/paradex-py/blob/b8248fb747e278d2167ac2f056b339a287d5ef30/paradex_py/api/sbe/paradex_1_0.xml)
at commit `b8248fb747e278d2167ac2f056b339a287d5ef30`, the schema docs.paradex.trade's
"Binary Encoding (SBE)" page names. The adapter negotiates schema 1:1
(`?sbeSchemaId=1&sbeSchemaVersion=1`), whose `BboEvent` and `TradeEvent` layouts are the ones
that file describes for versions below 2.

| File | What it is |
| --- | --- |
| `bbo.sbe.txt` | `BboEvent` (template 2), 48-byte root block |
| `bbo-ask-empty.sbe.txt` | The same with null ask price and size (no asks) |
| `bbo-longer-block.sbe.txt` | The same with a 56-byte root block: 8 bytes past the known fields, skipped |
| `trade.sbe.txt` | `TradeEvent` (template 1), 50-byte root block |
| `trade-longer-block.sbe.txt` | The same with a 58-byte root block and appended var data (`tradeIdStr`, as 1:2 appends it), both skipped |
| `heartbeat.sbe.txt` | `HeartbeatEvent` (template 40), skipped by the decoder |
| `book-longer-entries.sbe.txt` | A `BookEvent` (template 3) whose group entries are 24 bytes, 8 past the known price and size; only the SBE reader's group test reads it (the book is decoded in FBC-70f) |
