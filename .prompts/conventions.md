在编写代码时，请严格遵循以下规范：

1. 类型提示规范：
    - 所有函数参数必须包含类型注解
    - 所有函数返回值必须指定返回类型
    - 所有类属性必须声明类型

示例：
```python
def process_data(input_list: List[str], threshold: float = 0.5) -> Dict[str, Any]:
    pass
```

2. 文档规范：
    - 所有函数、类和模块必须包含 Google style 文档字符串
    - 文档必须包含以下部分：
        - 简短描述
        - 详细描述（如果需要）
        - Args（参数说明）
        - Returns（返回值说明）
        - Raises（异常说明，如果有）
    - 说明文档优先使用中文

示例：
```python
def process_data(input_list: List[str], threshold: float = 0.5) -> Dict[str, Any]:
    """处理输入的字符串列表并返回结果字典。

    对输入的字符串列表进行处理，根据阈值筛选并转换为指定格式。

    Args:
        input_list: 待处理的字符串列表
        threshold: 筛选阈值，默认为0.5

    Returns:
        包含处理结果的字典

    Raises:
        ValueError: 当输入列表为空时抛出
    """
    pass
```