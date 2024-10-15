
OversightML Tile Server
=======================

The OversightML Tile Server is a lightweight, cloud-based tile service which provides RESTful APIs for accessing
pixels and metadata for imagery stored in the cloud. Key features include:

- Works with imagery conforming to the `Cloud Optimized GeoTIFF (COG) <https://www.cogeo.org/>`_ and
  `National Imagery Transmission Format (NITF) <https://en.wikipedia.org/wiki/National_Imagery_Transmission_Format>`_
  standards
- Creates both orthophoto map and unwarped image tiles as needed from the source image pyramid
- Outputs image tiles in PNG, TIFF, JPEG formats and can also output NITFs for tiles without warping
- Conforms to the `OGC API - Tiles <https://ogcapi.ogc.org/tiles/>`_ specification

____

=========
REST APIs
=========

.. openapi:httpdomain:: _spec/openapi.yaml

____

===============================
Modules, Classes, and Functions
===============================

.. toctree::
   :maxdepth: 3

____

=======
Indices
=======

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
