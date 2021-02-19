# What
Thought I'd share my Python code that is part of my data engineering pipeline.

My Python codes extract data (in the form of an OData json) from Microsoft Graph and generates a tab-delimited file as output which can then be fed into a staging container (such as a blob storage, data lake) for further processing and transforms.

Consuming APIs with Python is relatively easy and if you couple that with numpy and pandas, we can further do data-driven workflows right off-the-bat!
