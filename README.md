# newsAPI

## 部署

### 环境

> **python**：`3.9+`
>
> **库**：`flask`，`BeautifulSoup4`，`requests`，`pymysql`，`apscheduler`

### 安装

```shell
pip install -r news-requirement.txt
```

### 运行

```shell
python3.9 runNews.py     # 不带flask
python3.8 runNews-flask.py    # 带flask版本，但只抓一个国家（测试）
```

## 接口

### 新闻查询接口

> 支持分页

**接口：**`/api/news`

**请求参数：**

| 参数名        | 参数值         | 备注                                  |
| ------------- | -------------- | ------------------------------------- |
| begin（可选） | 开始查询的位置 | 示例：<br />begin=5，从第五条开始查询 |
| limit（可选） | 查询条数       | 示例：<br />limit=15                  |

**返回值：**

> 前2个为通用参数

| 参数名    | 参数值     | 备注             |
| --------- | ---------- | ---------------- |
| code      | 8200,8500  | api返回状态码    |
| type      | "新闻查询" | 接口类型/描述    |
| timestamp | 1701586072 | 数据返回的时间戳 |

**DATA返回：**

| 参数名     | 返回值                                  | 备注       |
| ---------- | --------------------------------------- | ---------- |
| abstract   | 尽管最新业绩低于预期...                 | 新闻摘要   |
| content    | \                                       | 新闻正文   |
| created    | 1701316178000                           | 发布时间戳 |
| img        | /statics/692103.jpg                     | 新闻图片   |
| lang       | chinese                                 | 新闻语言   |
| nid        | 692103                                  | 文章id     |
| source_url | https://theedgemalaysia.com/node/692103 | 新闻源地址 |
| title      | 丰隆投行上修金轮企业评级和目标价        | 新闻标题   |

`res示例`：

```json
{
  "code": 8200,
  "data": [
    {
      "abstract": "（吉隆坡30日讯）尽管最新业绩低于预期，丰隆投资银行研究把金轮企业（Kimlun Corp Bhd）的“卖出”评级，上修至“守住”，目标价也从72仙，调高至79仙。",
      "content": "<div><div class=\"newsTextDataWrapInner\"><p>（吉隆坡30日讯）尽管最新业绩低于预期，丰隆投资银行研究把金轮企业（Kimlun Corp Bhd）的“卖出”评级，上修至“守住”，目标价也从72仙，调高至79仙。</p></div>\n<div class=\"newsTextDataWrapInner\"><p>该研究机构在报告中指出，新的目标价是基于2024财政年7倍的本益比得出。</p></div>\n<div class=\"newsTextDataWrapInner\"><p>该机构说，首9个月的核心净利暴跌97.9%至40万2000令吉，仅占其和市场全年预测的3.3%和1.7%。</p></div>\n<div class=\"newsTextDataWrapInner\"><p>“这是由于预制数量减少，因为一些新项目必须经过测试阶段，需要当局批准才能开始生产。据悉，一些配套已于11月投产。”</p></div>\n<div class=\"newsTextDataWrapInner\"><p>未入账订单为18亿6000万令吉，而今年迄今赢得的合约约为9亿2000万令吉，接近管理层2023财年10亿令吉的目标。</p></div>\n<div class=\"newsTextDataWrapInner\"><p>“由于新项目处于上升期，我们预计未来建筑赚幅将持平，如果柔佛的建筑合约流量强劲，金轮企业将处于有利位置。”</p></div>\n<div class=\"newsTextDataWrapInner\"><p>至于制造业务，丰隆投行预计第四季的表现将改善，因最近开始生产。</p></div>\n<div class=\"newsTextDataWrapInner\"><p>“制造订单为2亿9000万令吉，相信明年开始正常化，符合预测。”</p></div>\n<div class=\"newsTextDataWrapInner\"><p>该机构说，虽然赢得的合约优于预期，但近期盈利执行可能仍低迷。</p></div>\n<div class=\"newsTextDataWrapInner\"><p>截稿时，金轮企业跌2仙或2.5%，至78仙，市值报2亿7564万令吉。</p></div>\n<div class=\"newsTextDataWrapInner\"><p> </p></div>\n<div class=\"newsTextDataWrapInner\"><p>（编译：陈慧珊）</p></div>\n<div class=\"newsTextDataWrapInner\"><p> </p></div>\n\n</div>",
      "created": 1701316178000,
      "img": "/statics/692103.jpg",
      "lang": "chinese",
      "nid": "692103",
      "source_url": "https://theedgemalaysia.com/node/692103",
      "title": "丰隆投行上修金轮企业评级和目标价"
    }
  ],
  "timestamp": 1701586072.4698143,
  "type": "新闻查询"
}
```

### 新闻总数查询

**接口：**`/api/newsCount`

**请求参数：**

无

**DATA返回（只说明个别特别值）：**

| 参数名    | 返回值 | 备注     |
| --------- | ------ | -------- |
| newsCount | 26199  | 新闻总数 |

`res示例`：

```json
{
  "code": 8200,
  "data": 26199,
  "timestamp": 1701586375.511814,
  "type": "新闻总数查询"
}
```

### 新闻筛选

> 可根据传入参数筛选指定的新闻，如: nid=66981, title='马股持续下跌'

**接口：**`/api/filterNews`

**请求参数：**`列举两个，其他的类似`

| 参数名 | 参数值         | 备注                       |
| ------ | -------------- | -------------------------- |
| nid    | 66981          | 示例：nid=66981            |
| title  | '马股持续下跌' | 示例：title='马股持续下跌' |

**DATA返回：**

> 参考新闻查询接口