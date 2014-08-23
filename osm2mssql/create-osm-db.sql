CREATE DATABASE gis
GO

USE gis
GO

CREATE TABLE [dbo].[geometry_columns](
	[f_table_catalog] [varchar](256) NOT NULL,
	[f_table_schema] [varchar](256) NOT NULL,
	[f_table_name] [varchar](256) NOT NULL,
	[f_geometry_column] [varchar](256) NOT NULL,
	[coord_dimension] [int] NOT NULL,
	[srid] [int] NOT NULL,
	[geometry_type] [varchar](30) NOT NULL,
PRIMARY KEY ( [f_table_catalog], [f_table_schema], [f_table_name], [f_geometry_column] )
) 
GO

CREATE TABLE [dbo].[spatial_ref_sys](
	[srid] [int] NOT NULL,
	[auth_name] [varchar](256) NULL,
	[auth_srid] [int] NULL,
	[srtext] [varchar](2048) NULL,
	[proj4text] [varchar](2048) NULL,
PRIMARY KEY ( [srid] )
) 
GO

INSERT INTO dbo.spatial_ref_sys (srid, auth_name, auth_srid, srtext, proj4text)VALUES (900913,'EPSG',900913,'PROJCS["WGS84 / Simple Mercator",GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS_1984", 6378137.0, 298.257223563]],PRIMEM["Greenwich", 0.0],UNIT["degree", 0.017453292519943295],AXIS["Longitude", EAST],AXIS["Latitude", NORTH]],PROJECTION["Mercator_1SP_Google"],PARAMETER["latitude_of_origin", 0.0],PARAMETER["central_meridian", 0.0],PARAMETER["scale_factor", 1.0],PARAMETER["false_easting", 0.0],PARAMETER["false_northing", 0.0],UNIT["m", 1.0],AXIS["x", EAST],AXIS["y", NORTH],AUTHORITY["EPSG","900913"]]','+proj=merc +a=6378137 +b=6378137 +lat_ts=0.0 +lon_0=0.0 +x_0=0.0 +y_0=0 +k=1.0 +units=m +nadgrids=@null +no_defs');
GO

