# 2. Target .NET framwork 4.5.1

Date: 12/02/2016

## Status

Accepted

## Context

Which .NET framework to target for libraries that are shipped.

## Decision

.NET 4.5.1 is the lowest .NET framework we will target. This will give
us the maximum portability for consumers. Test projects may target 
higher version if desired.

## Consequences

Consumers using .NET frameworks less than .NET 4.5.1 won't be 
supported/.