# 为什么要关注 Necroflow？

逻辑由你用 Python 写。缓存、DAG 和路径的谱系交给框架。

## 流水线就是一个普通的 Python 脚本

```python
@command("tr '[:lower:]' '[:upper:]' < {raw_text} > {processed_text}")
def process_text(raw_text: RawText, tool_config: ToolConfig):
    processed_text = output(ProcessedText)
    return processed_text

def my_pipeline(P: Pipeline, config: dict) -> None:
    P.raw = import_text(P, path=config["input"])
    P.processed = process_text(P, P.raw, P.tool_config)
    P.summary = summarize(P, P.processed)
```

- 结果就是普通变量，规则就是普通函数。加一个步骤 = 加一次调用，再对变量的用法做一点局部改动，
  不必重写文件名。
- 构图阶段可用完整的 Python：循环、条件、函数、pytest 测试。子流水线（`P.subpipeline`）就是普通
  的工厂函数，共享前缀只算一次。
- 读起来像普通的过程式代码。这在代码评审时尤其重要：改动都集中在调用处，所以很容易检查 AI agent
  是否正确理解了意图——或者你自己写好逻辑，把规则的实现交给别人。流水线就像一个 C++ 头文件，
  声明事情应该如何运作。

## 路径由框架生成

- 结果落在 `nodes/{规则}/{provenance_hash}/{文件}`。该哈希（fingerprint v4）覆盖配方结构、配置、
  shell 以及完整的父节点谱系。
- 相同的计算会收敛到同一个目录——同一个 DAG 中相互独立的流水线之间也是如此。你不需要设计文件名
  分类体系，也不需要通配符。
- 结果旁边是 `.rip/`：`dependencies.toml`（谱系 + 所消费父节点的 SHA-256）、`graph.tgf`（祖先
  DAG）、`job.log`、`state`、`run.toml`（耗时、大小）。路径的谱系随文件一起走，而不是躺在中心
  数据库里。
- 你想查看的东西，会以副本形式出现在 `results/<job>/` 下，带有可读的标签，另有 `manifest.toml`
  （可见路径、来源节点、内容哈希）。只要文件系统允许，副本就用 reflink/CoW。

## 我们按内容缓存，而不是按时间

- 父节点重建后字节完全相同，**不会**让消费者失效：我们比较 SHA-256，mtime 只用于让快速路径失效。
- 状态存在文本文件里，没有数据库。崩溃后残留的 `running` 会强制重跑。
- `necroflow explain job.toml` 会说明将要运行什么、为什么（逐节点）；`doctor` 做预检并给出稳定的
  `NF_*` 代码；`gc` 清理 node store；`graph --json` / `outputs --json` / `provenance --json`
  面向工具和 agent。

## 带类型的结果

- `NodeType` 就是文件类型。层次结构由你决定；子类型和类型的联合（可选分支）充当格式契约，而
  `filename = None` 给出只能用作输入的抽象契约。
- 框架在流水线构建期间就检查组合是否合法——错误会在任何昂贵的计算启动之前抛出。

## 运行方式

- 一个 `job.toml` 描述一次运行；`__grid` 把参数网格（调参、多个数据集）展开成带确定性标签的独立
  作业。
- 本地并行执行器，带资源上限（`threads`、`ram` 以及自定义资源）、调度器协议（默认 FIFO）、
  `--dry-run`、`--keep-going`、`repeat=N`，以及针对中间结果的 `autoclean`。
- `RuleCall` 是原子的：一次调用的所有共同输出一起缓存、一起执行。退出码为 0 但声明的文件缺失，
  算失败，不算成功。

## 与 Nextflow（以及 Snakemake）相比如何？

量级不同，更重要的是抽象边界不同：

- 一台机器。我们不协调集群——目前还不。
- 容器是正交的：Necroflow 并不以任何特定方式使用它们。你可以把整个项目封进一个镜像，也可以让个别
  规则基于 `docker run`，把 Necroflow 的便利和外部配置好的环境的可复现性结合起来。
- 用 Python 而不是 Groovy 或通配符 DSL：对开发者、数据科学家和 AI 都更简单。测试和跨多数据集的
  流水线用常规手段就能写。
- Snakemake 是最接近的参照，而且很成熟。差别不在于那边做不到——而在于当变体不断堆积时，由谁来维护
  路径分类体系和通配符约束。
- Prefect 证明了 Python 里的动态编排行得通，但通用 task 既不提供带类型的结果文件，也不提供由谱系
  推导出的路径。
- 代码量小：`src/` 里约 5.6 千行。

缺点：这是新东西，所以你自动成为早期采用者；没有 HPC，也没有云；没有插件生态。

## 近期计划

- 论文——正在写。
- 复活基于 Textual 的进程管理器。现在规则的 stdout 进入 `.rip/job.log`（因为多条规则同时在跑），
  `tail` 能用，但终究不等于一个窗口管理器。
- 摸一摸 SLURM——我住处附近就有一台超算，会去看看它和现有的东西如何衔接。
