# arc-plugin-sandbox-stream

This plugin enables developers to use DynamoDB streams locally in their arc or enhance sandbox environment

Note you'll need to running DynamoDB Local and have it configured in architect, [please refer to my guide](https://martinhicks.net/articles/arc-sandbox-table-streams/). 

## Setup 

__1. Add the dependency to your project__

`npm install @hicksy/arc-plugin-sandbox-stream`

__2. Configure your project to use @tables-streams in `.arc` file__

```
@tables
example
  pk *String
  sk **String

@tables-streams
example
```
__3. Add the plugin to arc config__


```
@plugins
hicksy/arc-plugin-sandbox-stream
```