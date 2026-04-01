# Domain Breakdown Implementation

## 1. 目标

当前 `market_annotations.py` 会先解析 `domain_parsed` 与 `sub_domain`，再基于 `domain_parsed` 生成最终 `domain`。现阶段 `ncaa.com`、`binance.com`、`liquipedia.net` 的市场量过大，需要把它们进一步拆分成更细的 domain key，便于后续 rule generation 在这些来源内继续分桶。

本次改动的目标只有一个：

- 在不改变其他功能的前提下，为特定 domain 生成更细粒度的 `domain`

明确保持不变的内容：

- `source_url` 的提取逻辑不变
- `domain_parsed` 的解析逻辑不变
- `sub_domain` 的解析逻辑不变
- `category`、`category_raw`、`category_parsed` 的生成逻辑不变
- `market_type`、`outcome_pattern` 的生成逻辑不变
- 低频 domain 折叠为 `OTHER` 的机制保留，只是统计对象从旧的 `domain_parsed` 切换到新的细粒度 candidate domain
- `save_market_annotations()`、`build_other_outcome_patterns_by_url()`、快照 merge 逻辑保持兼容

## 2. 当前行为

当前 `build_market_annotations()` 的关键步骤如下：

1. 基于 `resolutionSource` 或 `description` 生成 `source_url`
2. 从 `source_url` 解析出 `domain_parsed` 和 `sub_domain`
3. 基于 `domain_parsed` 与 `gameId` 推断 `category_parsed`
4. 使用 `domain_parsed` 统计频次，并生成最终 `domain`

当前最终 `domain` 的来源是：

```python
df["domain"] = df["domain_parsed"].apply(normalize_domain)
```

这意味着：

- 所有 `ncaa.com` 市场都会落到同一个 domain
- 所有 `binance.com` 市场都会落到同一个 domain
- 所有 `liquipedia.net` 市场都会落到同一个 domain

这会导致后续规则训练时，这几个来源内部的不同子簇无法被拆开。

## 3. 目标行为

引入一个新的“细粒度 domain candidate”概念。最终 `domain` 不再直接等于 `domain_parsed`，而是先按规则生成一个更细的 candidate，再进行低频折叠。

逻辑顺序应为：

1. 先得到现有的 `domain_parsed` 与 `sub_domain`
2. 针对少数特定 domain 生成 `domain_candidate`
3. 用 `domain_candidate` 统计频次
4. 对低频 candidate 继续折叠到 `OTHER`
5. 输出最终 `domain`

建议新增一个纯函数，职责单一：

```python
def build_domain_candidate(row: pd.Series) -> str:
    ...
```

输入至少依赖以下列：

- `domain_parsed`
- `sub_domain`
- `description`

输出：

- 细粒度 domain candidate

如果不命中特殊规则，直接返回原始 `domain_parsed`。

## 4. 规则定义

### 4.1 `ncaa.com`

#### 目标

从 `description` 中提取运动名称，并拼接到 `domain` 后面：

- `ncaa.com.basketball`
- `ncaa.com.football`
- `ncaa.com.baseball`

如果无法可靠提取，则保持为：

- `ncaa.com`

本次只检查以下 7 个 sport：

- `baseball`
- `basketball`
- `volleyball`
- `football`
- `soccer`
- `swimming`
- `tennis`

除这 7 个之外，其他 NCAA 运动一律不继续细分，统一保留为 `ncaa.com`。

#### 原因

`ncaa.com` 下不同运动的数据量都很大，但规则生成时这些运动的市场结构、赛季节奏、流动性和定价行为可能不同。把 sport 作为 domain key 的一部分，可以让后续 rule generation 自动把不同运动拆开。

#### 建议实现

新增 sport 提取函数，例如：

```python
def extract_ncaa_sport_from_description(description: str) -> str | None:
    ...
```

建议处理步骤：

1. 将 `description` 转小写
2. 仅用固定关键词词典匹配这 7 个 NCAA 运动名称
3. 命中后返回标准化 sport token
4. 未命中返回 `None`

建议的标准化 sport token 只保留以下 7 个：

- `baseball`
- `basketball`
- `volleyball`
- `football`
- `soccer`
- `swimming`
- `tennis`

建议优先采用显式关键词匹配，不要做模糊 NLP 推断，避免把实现变复杂且不可控。

#### 推荐关键词映射

可维护一个静态映射表，例如：

```python
NCAA_SPORT_KEYWORDS = {
    "baseball": ["baseball"],
    "basketball": ["basketball", "march madness"],
    "volleyball": ["volleyball"],
    "football": ["football"],
    "soccer": ["soccer"],
    "swimming": ["swimming", "swim"],
    "tennis": ["tennis"],
}
```

生成规则：

- 若 `domain_parsed != "ncaa.com"`，不触发
- 若 `domain_parsed == "ncaa.com"` 且识别出 sport，则输出 `f"ncaa.com.{sport}"`
- 若未识别出 sport，则输出 `"ncaa.com"`

#### 示例

| domain_parsed | description | 输出 domain candidate |
|---|---|---|
| `ncaa.com` | `NCAA Men's Basketball Championship odds` | `ncaa.com.basketball` |
| `ncaa.com` | `Will this college football team win the title?` | `ncaa.com.football` |
| `ncaa.com` | `NCAA tournament futures` | `ncaa.com` |

最后一个例子表示：描述中没有稳定的 sport 关键词时，不强行细分。

如果 description 指向其他 NCAA 项目，例如 hockey、lacrosse、softball、wrestling 等，也统一保留 `ncaa.com`，不生成新的细分 domain。

### 4.2 `binance.com`

#### 目标

当 `sub_domain` 以 `USDT` 结尾时，将 `domain` 改为：

- `binance.com<sub_domain>`

例如：

- `binance.com./en/trade/BTC_USDT`
- `binance.com./price/ETHUSDT`

如果 `sub_domain` 不以 `USDT` 结尾，则保持：

- `binance.com`

#### 原因

`binance.com` 内部可能对应不同交易对页面。对于以 `USDT` 结尾的路径，通常已经明确指向某个币对，可以用 `domain + sub_domain` 进一步拆分。

#### 关键约束

这里的判断基于现有的 `sub_domain` 结果，不修改 `normalize_sub_domain()` 的行为。

也就是说：

- 先沿用现有 `sub_domain` 抽取规则
- 再在生成 `domain_candidate` 时检查 `sub_domain.endswith("USDT")`

为了避免大小写问题，建议判断时使用：

```python
sub_domain.upper().endswith("USDT")
```

生成规则：

- 若 `domain_parsed != "binance.com"`，不触发
- 若 `domain_parsed == "binance.com"` 且 `sub_domain` 非空并以 `USDT` 结尾，则输出 `f"binance.com{sub_domain}"`
- 否则输出 `"binance.com"`

#### 示例

| domain_parsed | sub_domain | 输出 domain candidate |
|---|---|---|
| `binance.com` | `/price/BTCUSDT` | `binance.com/price/BTCUSDT` |
| `binance.com` | `/trade/ETH_USDT` | `binance.com/trade/ETH_USDT` |
| `binance.com` | `/trade/ETH_BTC` | `binance.com` |
| `binance.com` | `` | `binance.com` |

### 4.3 `liquipedia.net`

#### 目标

始终使用 `domain + sub_domain` 作为 candidate domain：

- `liquipedia.net<sub_domain>`

如果 `sub_domain` 为空，则保持：

- `liquipedia.net`

#### 原因

`liquipedia.net` 的一级路径本身就是比较稳定的子领域切分，通常对应不同电竞项目。当前 `normalize_sub_domain()` 已经对 `liquipedia.net` 取第一段 path，因此可以直接复用。

生成规则：

- 若 `domain_parsed != "liquipedia.net"`，不触发
- 若 `domain_parsed == "liquipedia.net"` 且 `sub_domain` 非空，则输出 `f"liquipedia.net{sub_domain}"`
- 若 `sub_domain` 为空，则输出 `"liquipedia.net"`

#### 示例

| domain_parsed | sub_domain | 输出 domain candidate |
|---|---|---|
| `liquipedia.net` | `/counterstrike` | `liquipedia.net/counterstrike` |
| `liquipedia.net` | `/dota2` | `liquipedia.net/dota2` |
| `liquipedia.net` | `` | `liquipedia.net` |

## 5. 建议代码落点

建议仅在 [market_annotations.py](market_annotations.py) 中做最小改动。

### 5.1 新增 helper

建议新增以下 helper：

```python
NCAA_SPORT_KEYWORDS = {...}

def extract_ncaa_sport_from_description(description: str) -> str | None:
    ...

def build_domain_candidate(row: pd.Series) -> str:
    ...
```

### 5.2 在 `build_market_annotations()` 中插入点

建议在以下位置之后插入：

```python
df["domain_parsed"] = parsed.apply(lambda value: value[0])
df["sub_domain"] = parsed.apply(lambda value: value[1])
df["source_url"] = parsed.apply(lambda value: value[2] or UNKNOWN)
```

紧接着新增：

```python
df["domain_candidate"] = df.apply(build_domain_candidate, axis=1)
```

然后把原有的：

```python
domain_counts = Counter(domain for domain in df["domain_parsed"] if domain not in {"", UNKNOWN})
```

改为：

```python
domain_counts = Counter(domain for domain in df["domain_candidate"] if domain not in {"", UNKNOWN})
```

再把：

```python
df["domain"] = df["domain_parsed"].apply(normalize_domain)
```

改为：

```python
df["domain"] = df["domain_candidate"].apply(normalize_domain)
```

### 5.3 不建议改动的部分

以下逻辑不建议动：

- `MarketSourceParser.normalize_sub_domain()`
- `MarketSourceParser.parse_domain_parts()`
- `infer_category_from_source()`
- `columns` 输出字段集合

原因：本次需求只要求“把部分 domain breakdown 成更细的 domain”，不要求扩展 schema，也不要求重定义 `domain_parsed` 的语义。

`domain_parsed` 继续保留“原始解析出来的主域名”更清晰，`domain` 继续保留“供下游使用的最终 domain key”。

## 6. 低频折叠的预期变化

当前低频折叠是按粗粒度域名做的。引入细分后，`LOW_FREQUENCY_DOMAIN_COUNT` 的判定对象会变成更细粒度的 candidate domain，因此会产生以下结果：

- `ncaa.com.basketball`、`ncaa.com.football` 等会单独计数
- 某些样本量不足的细分 domain 可能被折叠成 `OTHER`
- `liquipedia.net/某个游戏` 也会单独计数
- `binance.com./某个USDT页面` 也会单独计数

这符合需求目标，因为我们要的是“有足够样本的子 domain 自动保留，否则仍按系统已有机制回退”。

## 7. 对下游的影响

### 7.1 会变化的部分

- `market_domain_features.csv` 中的 `domain` 列会出现更细粒度值
- `domain_summary.csv` 会按更细粒度的 `domain` 聚合
- snapshots merge 后，下游训练/分析使用的 `domain` 也会随之细化
- 后续 rule generation 若按 `domain` 分组，会自然把这些子来源拆开

### 7.2 不会变化的部分

- `domain_parsed` 仍保留原始主域名，如 `ncaa.com`、`binance.com`、`liquipedia.net`
- `sub_domain` 的生成与保存方式不变
- `category` 推断仍基于 `domain_parsed` 和 `gameId`
- `market_type` 与 `outcome_pattern` 完全不受影响
- `other_outcome_patterns_by_url.csv` 的 schema 不变

## 8. 边界条件

### 8.1 `ncaa.com`

- `description` 为空时，不报错，直接回退到 `ncaa.com`
- `description` 含多个 sport 关键词时，建议按映射表遍历顺序取第一个命中项
- 只允许 `baseball`、`basketball`、`volleyball`、`football`、`soccer`、`swimming`、`tennis` 这 7 个输出为细分 domain
- 其他 NCAA 项目即使被识别到，也不输出细分 domain，而是回退到 `ncaa.com`
- 不建议使用过宽泛词，如 `tournament`、`championship` 作为独立 sport 判断依据

### 8.2 `binance.com`

- `sub_domain` 为空时，回退到 `binance.com`
- 只在 `sub_domain` 以 `USDT` 结尾时拆分，不额外处理 `USDC`、`BTC` 等后缀，除非后续再提需求
- 保持当前字符串拼接形式，不额外清洗 `/` 或 `_`，避免改变既有 `sub_domain` 语义

### 8.3 `liquipedia.net`

- `sub_domain` 为空时，回退到 `liquipedia.net`
- 由于当前 `normalize_sub_domain()` 已对 `liquipedia.net` 截取第一段 path，所以这里直接拼接即可

## 9. 建议测试用例

建议至少补一组单元测试，覆盖 `build_market_annotations()` 输出中的 `domain` 列。

最低覆盖集：

1. `ncaa.com` + 可识别 basketball description -> `ncaa.com.basketball`
2. `ncaa.com` + 可识别 swimming description -> `ncaa.com.swimming`
3. `ncaa.com` + 可识别 tennis description -> `ncaa.com.tennis`
4. `ncaa.com` + 其他 NCAA sport description -> `ncaa.com`
5. `ncaa.com` + 不可识别 sport -> `ncaa.com`
6. `binance.com` + `sub_domain` 以 `USDT` 结尾 -> `binance.com<sub_domain>`
7. `binance.com` + 非 `USDT` 结尾 -> `binance.com`
8. `liquipedia.net` + 非空 `sub_domain` -> `liquipedia.net<sub_domain>`
9. `liquipedia.net` + 空 `sub_domain` -> `liquipedia.net`
10. 非目标 domain -> 与当前行为完全一致
11. 细分 candidate 样本量低于阈值 -> 最终 `domain == "OTHER"`

如果已有针对 market annotations 的测试文件，优先在现有测试文件中补 case；如果没有，再新增单独测试文件。

## 10. 推荐实现摘要

建议实现可以概括为一句话：

- 保持 `domain_parsed` 和 `sub_domain` 的现有定义不变，只在生成最终 `domain` 之前增加一个 `domain_candidate` 层，并仅对 `ncaa.com`、`binance.com`、`liquipedia.net` 做定制化细分。

推荐的最终伪代码：

```python
parsed = df["source_url"].apply(MarketSourceParser.parse_domain_parts)
df["domain_parsed"] = parsed.apply(lambda value: value[0])
df["sub_domain"] = parsed.apply(lambda value: value[1])
df["source_url"] = parsed.apply(lambda value: value[2] or UNKNOWN)

df["domain_candidate"] = df.apply(build_domain_candidate, axis=1)

domain_counts = Counter(
    domain for domain in df["domain_candidate"] if domain not in {"", UNKNOWN}
)

df["domain"] = df["domain_candidate"].apply(normalize_domain)
```

这样可以把改动范围压缩在最终 domain 生成环节，既满足新的 breakdown 需求，也能最大限度保证其余逻辑保持不变。