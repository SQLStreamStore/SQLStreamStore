
# Browse Streams Schema

```
```


| Abstract | Extensible | Status | Identifiable | Custom Properties | Additional Properties | Defined In |
|----------|------------|--------|--------------|-------------------|-----------------------|------------|
| Can be instantiated | No | Experimental | No | Forbidden | Forbidden | [feed-browser.schema.json](feed-browser.schema.json) |

# Browse Streams Properties

| Property | Type | Required | Defined by |
|----------|------|----------|------------|
| [c](#c) | `string` | Optional | Browse Streams (this schema) |
| [m](#m) | `integer` | Optional | Browse Streams (this schema) |
| [p](#p) | `string` | Optional | Browse Streams (this schema) |
| [t](#t) | `enum` | Optional | Browse Streams (this schema) |

## c

A continuation token. Used for paging.

`c`
* is optional
* type: `string`
* defined in this schema

### c Type


`string`






## m
### Max Count

The maximum number of results to return.

`m`
* is optional
* type: `integer`
* defined in this schema

### m Type


`integer`
* minimum value: `1`
* maximum value: `100`





## p
### Pattern

The pattern to search for.

`p`
* is optional
* type: `string`
* defined in this schema

### p Type


`string`






## t
### Pattern Type

The pattern type. Valid values are s (startsWith), e (endsWith), or null (match any stream).

`t`
* is optional
* type: `enum`
* defined in this schema

The value of this property **must** be equal to one of the [known values below](#t-known-values).

### t Known Values
| Value | Description |
|-------|-------------|
| `s` |  |
| `e` |  |
| `null` |  |



