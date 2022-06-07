# Muninn

Muninn is a library and a set of command-line tools to create and manage
data product catalogues and archives. It can function as a pure product
metadata catalogue or it can manage a product archive together with its
metadata catalogue.

When using a product archive, muninn can automatically extract properties
from the products when products get added to the archive. Automatic property
extraction is handled through product type specific plug-ins
(see [Extensions](https://stcorp.github.io/muninn/extensions/)), which are
*not* included in the muninn distribution.

Muninn uses the concept of namespaces to group different sets of properties
for a product together within the catalogue. Muninn itself provides a 'core'
namespace that covers the most common properties for data products.
Support for additional namespaces are handled through external plug-ins
(see [Extensions](https://stcorp.github.io/muninn/extensions/)).

In Norse mythology, Muninn is a raven that, together with another raven called
Huggin, gather information for the god Odin. Muninn is Old Norse for "memory".

# Documentation

See the [online documentation](https://stcorp.github.io/muninn/).
