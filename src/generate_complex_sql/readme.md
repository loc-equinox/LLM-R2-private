# Generate complex sql query
思路：使用llm生成sql中部分内容使之复杂化，后拼接

## 步骤
### step 1

处理原始查询（llmr2中给出的训练/测试查询），分解子查询并存储 get_deepest_subquery.py

**todo**：

代码中的示例仅处理了最深一层子查询，可泛化到其他指定子查询层次

### step 2

执行connect_gpt_subquery.py生成复杂子查询/connect_gpt-where-parallel.py生成复杂并行子查询

**todo**：
1. 可以继续探索完善这一部分内容（生成where条件），迭代多次以生成更加深/并行更多/更复杂的子查询
2. 除了对于where条件部分的处理，尝试在select部分、from部分增加复杂性，添加子查询/复杂条件

### step 3

使用sqlglot工具，执行replace_where_condition.py 将生成的子查询拼回原始查询；执行replace_deepest_subquery.py 拼回最深一层子查询处


### step 4

筛选查询：1.是否足够复杂（llmr2推荐不出有效规则---执行llmr2算法） 2.是否语法正确（执行filter_correct_sql.py）

作为反馈信息 迭代重新生成

