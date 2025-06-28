
CREATE TABLE orders(
	[row_id] [float] NULL,
	[order_id] [nvarchar](255) NULL,
	[order_date] [date] NULL,
	[ship_date] [date] NULL,
	[ship_mode] [nvarchar](255) NULL,
	[customer_id] [nvarchar](255) NULL,
	[customer_name] [nvarchar](255) NULL,
	[segment] [nvarchar](255) NULL,
	[country] [nvarchar](255) NULL,
	[city] [nvarchar](255) NULL,
	[state] [nvarchar](255) NULL,
	[postal_code] [float] NULL,
	[region] [nvarchar](255) NULL,
	[product_id] [nvarchar](255) NULL,
	[category] [nvarchar](255) NULL,
	[sub_category] [nvarchar](255) NULL,
	[product_name] [nvarchar](255) NULL,
	[sales] [float] NULL,
	[quantity] [float] NULL,
	[discount] [float] NULL,
	[profit] [float] NULL
) 
;