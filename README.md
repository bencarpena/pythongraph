![Header](https://github.com/bencarpena/pythongraph/blob/main/graph_python.png)

# What
Thought I'd share my Python code that is part of my data engineering pipeline.

My Python codes extract data (in the form of an OData json) from Microsoft Graph and generates a tab-delimited file as output which can then be fed into a staging container (such as a blob storage, data lake) for further processing and transforms.

Consuming APIs with Python is relatively easy and if you couple that with numpy and pandas, we can further do data-driven workflows right off-the-bat!

# Here it is
>Keep an eye out for an Easter egg :)

https://github.com/bencarpena/pythongraph/blob/main/get_graphusers_amplify.py

# Solution Architecture and Diagram


![Solution Architecture](https://github.com/bencarpena/pythongraph/blob/main/engineering.png)
