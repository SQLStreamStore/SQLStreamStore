
# Append to Stream Schema

```
```


| Abstract | Extensible | Status | Identifiable | Custom Properties | Additional Properties | Defined In |
|----------|------------|--------|--------------|-------------------|-----------------------|------------|
| Can be instantiated | No | Experimental | No | Forbidden | Forbidden | [append.schema.json](append.schema.json) |

# Append to Stream Properties

| Property | Type | Required | Defined by |
|----------|------|----------|------------|
| [jsonData](#jsondata) | `object` | **Required** | Append to Stream (this schema) |
| [jsonMetadata](#jsonmetadata) | `object` | Optional | Append to Stream (this schema) |
| [messageId](#messageid) | `string` | **Required** | Append to Stream (this schema) |
| [type](#type) | `string` | **Required** | Append to Stream (this schema) |

## jsonData


`jsonData`
* is **required**
* type: `object`
* defined in this schema

### jsonData Type


`object` with following properties:


| Property | Type | Required |
|----------|------|----------|






## jsonMetadata


`jsonMetadata`
* is optional
* type: `object`
* defined in this schema

### jsonMetadata Type


`object` with following properties:


| Property | Type | Required |
|----------|------|----------|






## messageId


`messageId`
* is **required**
* type: `string`
* defined in this schema

### messageId Type


`string`


All instances must conform to this regular expression 
(test examples [here](https://regexr.com/?expression=%5E%5B0-9A-Fa-f%5D%7B8%7D-%5B0-9A-Fa-f%5D%7B4%7D-%5B0-9A-Fa-f%5D%7B4%7D-%5B0-9A-Fa-f%5D%7B4%7D-%5B0-9A-Fa-f%5D%7B12%7D%24)):
```regex
^[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}$
```






## type


`type`
* is **required**
* type: `string`
* defined in this schema

### type Type


`string`





