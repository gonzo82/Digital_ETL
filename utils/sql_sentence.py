HUBSPOT_USERS_DATA ="""
with
coupon_code as (
	select
		cc.cart_id,
		max(c.code) as code
	from
		comprea.cart_coupon cc
			inner join
		comprea.coupon c on cc.coupon_id = c.id
	group by 1
),
city_country as (
	select
		city,
		min(case
				when city = 'Bucharest' then 'Romania'
				else replace(country, 'España', 'Spain')
			end
		) as country
	from
		comprea.postalcode
	where
		country is not null
		and city is not null
		and country <> 'Singapur'
		and country <> 'Estados Unidos'
	group by 1
),
incidences as (
	select
		ae.cart_id,
		max(
			case when ae.problem_type in('cold_chain_break','item_damaged',
						'item_lost_but_paid','item_missed_but_charged','late',
						'late_delivery','no_call','wrong_item')
				then 1
			else 0
			end
		) = 1 as has_incidence,
		count(*) > 0 as has_any_incidence
	from
		comprea.audit_email ae
	where
		ae.cart_id is not null
		and ae.problem_type is not null
		and ae.problem_type <> ''
	group by 1
),
product_incidences as (
	select
		cp.cart_id
	from
		comprea.product_incident pi
			inner join
		comprea.cart_product cp on pi.cart_product_id = cp.id
	group by 1
),
reschedules as (
	select
		str.cart_id
	from
		comprea.shopper_timeslot_reassignment str
			inner join
		comprea.shopper_timeslot st on str.previous_shopper_timeslot_id = st.id
			inner join
		comprea.shopper_timeslot st_new on str.new_shopper_timeslot_id = st_new.id
	where
		st.start_datetime <> st_new.start_datetime
	group by 1
),
mgm as (
	select
		coupon.user_id,
		date_add('seconds', max(c.date_delivered), '1970-01-01')::date as dt_mgm
	from
		comprea.cart c
			inner join
		comprea.cash_order co on c.cash_order_id = co.id
			inner join
		comprea.cart_coupon cc on c.id = cc.cart_id
			inner join
		comprea.coupon coupon on cc.coupon_id = coupon.id
			inner join
		comprea.user u on coupon.user_id = u.id
	where
		c.status = 'delivered'
		and coupon.type = 'person'
		and co.user_id <> coupon.user_id
		and c.date_delivered is not null
		and c.date_delivered >= date_diff('seconds', '1970-01-01', CURRENT_DATE - interval '1 week')
	group by 1
),
ratings as (
	select
		co.user_id,
		sb.rating,
		sb.date,
		rank() over (partition by co.user_id order by sb.date desc, sb.id desc) as ranking
	from
		comprea.shopper_bill sb
			inner join
		comprea.cart c on sb.cart_id = c.id
			inner join
		comprea.cash_order co on c.cash_order_id = co.id
	where
		sb.rating is not null
),
available_postal_code as (
	select
		distinct p.value as postal_code
	from
		comprea.shop_postalcode sp
			inner join
		comprea.postalcode p on sp.postalcode_id = p.id
			inner join
		comprea.shop s on sp.shop_id = s.id and s.is_active =1
),
ratings_5 as (
	select
		user_id,
		date_add('seconds', max(ratings.date), '1970-01-01')::date as last_rating
	from
		ratings
	group by 1
	having
		max(case when ranking = 1 then rating else 0 end) = 5
		and max(case when ranking = 1 then rating else 0 end) = 5
		and max(case when ranking = 1 then rating else 0 end) = 5
		and max(case when ranking = 1 then rating else 0 end) = 5
		and max(case when ranking = 1 then rating else 0 end) = 5
),
carts_pre as (
	select
		co.user_id,
		rank() over (partition by co.user_id order by c.status = 'delivered' desc, coalesce(st.start_datetime, 0) desc, c.date_delivered desc, c.last_update desc, c.id desc) as ranking_last_or,
		rank() over (partition by co.user_id order by c.expected_delivering_time desc, c.last_update desc, c.id desc) as ranking_last_or_all,
		coalesce(oa.name, oau.name) as city,
		case
			when coalesce(oa.name, oau.name) = 'ES' then 'Spain'
			when coalesce(oa.name, oau.name) = 'RO' then 'Romani'
			when coalesce(oa.name, oau.name) = 'HU' then 'Hungary'
			else coalesce(p.country, ccountry.country, '')
		end as country,
		case when c.status = 'delivered' then 1 else 0 end as ors,
		coalesce(p.value, pu.value) as postal_code,
		c.date_delivered,
		c.last_update,
		case
			when c.status = 'delivered' then coalesce(c.shopper_total_price, 0) + coalesce(co.delivery_price, 0)
			else 0
		end as price,
		case
			when c.status = 'delivered' then coalesce(c.total_price_discount, 0) + coalesce(co.delivery_price_discount, 0)
			else 0
		end as discount,
		co.device_method,
		cc.code as coupon_code,
		c.status,
		sb.rating,
		s.company_id,
		c2.name as company_name,
		case
			when c.status = 'delivered' then 
				date_diff('hour', date_add('seconds', co.finish_datetime, '1970-01-01'), date_add('seconds', st.start_datetime, '1970-01-01')) <= 2
			else false
		end as express,
		c.date_delivered < st.finish_datetime as otd,
		coalesce(inci.has_incidence, false) as has_incidence,
		coalesce(not inci.has_incidence and inci.has_any_incidence, false) as has_other_incidence,
		coalesce(pi.cart_id is not null, false) as has_product_incidence,
		coalesce(res.cart_id is not null, false) as has_reschedules
	from
		comprea.cart c
			inner join
		comprea.cash_order co on c.cash_order_id = co.id
			inner join
		comprea.user u on co.user_id = u.id
			left join
		comprea.postalcode pu on u.postalcode_id = pu.id
			left join
		comprea.operational_area oau on pu.operational_area_id = oau.id
			left join
		comprea.shopper_timeslot st on c.shopper_timeslot_id = st.id
			left join
		comprea.shopper_bill sb on c.id = sb.cart_id and st.shopper_id = sb.shopper_id
			left join
		comprea.address a on c.address_id = a.id
			left join
		comprea.postalcode p on a.postalcode_id = p.id
			left join
		comprea.operational_area oa on p.operational_area_id = oa.id
			left join
		city_country ccountry on p.city = ccountry.city
			left join
		coupon_code cc on c.id = cc.cart_id
			left join
		comprea.shop s on c.shop_id = s.id
			left join
		comprea.company c2 on s.company_id = c2.id
			left join
		incidences inci on c.id = inci.cart_id
			left join
		product_incidences pi on c.id = pi.cart_id
			left join
		reschedules res on c.id = res.cart_id
	where c.status <> 'deleted'
),
carts as (
	select
		user_id,
		ranking_last_or,
		ranking_last_or_all,
		case when city = 'Cluj' then 'Bucharest' else city end as city,
		ors,
		case
			when coalesce(country, '') = 'Romania' then right((1000000 + postal_code::int)::varchar, 6)
			when coalesce(country, '') <> 'España' and coalesce(country, '') <> 'Spain' then postal_code
			when postal_code = '' then null
			when REGEXP_COUNT(postal_code, '[0-9][0-9][0-9][0-9]') = 0 then null
			when REGEXP_COUNT(lower(postal_code), '[a-z]') > 0 then null
			when postal_code::int < 1000 then null
			when postal_code::int > 99999 then null
			else right((100000 + postal_code::int)::varchar, 5)
		end as postal_code,
		date_delivered,
		last_update,
		price,
		discount,
		device_method,
		coupon_code,
		status,
		rating,
		company_id,
		company_name,
		express,
		otd,
		has_incidence,
		has_other_incidence,
		has_product_incidence,
		has_reschedules
	from
		carts_pre
),
carts_per_user as (
	select
		c.user_id,
		max(apc.postal_code) is not null as service_available,
		max(case when c.ranking_last_or = 1 then c.city end) as city,
		max(case when c.ranking_last_or = 1 then c.postal_code end) as postal_code,
		sum(c.ors) as ors,
		date_add('seconds', min(c.date_delivered), '1970-01-01') as first_date_delivered,
		date_add('seconds', max(c.date_delivered), '1970-01-01') as last_date_delivered,
		date_add('seconds', max(c.last_update), '1970-01-01') as last_deal_date,
		sum(c.price) as price,
		sum(c.discount) as discount,
		sum(coalesce(c.price, 0) - coalesce(c.discount, 0)) as gmv,
		max(case when c.ranking_last_or = 1 then c.device_method end) as device_method,
		round(case when coalesce(sum(c.ors), 0) = 0 then null else sum(coalesce(c.price, 0) - coalesce(c.discount, 0)) / sum(c.ors) end, 2) as avg_cart,
		max(case when c.ranking_last_or = 1 then c.coupon_code end) as coupon_code,
		max(case when c.ranking_last_or = 1 then c.rating end) as last_rating,
		count(distinct case when c.status = 'delivered' then c.company_id else null end) > 1 as multitienda,
		max(case when c.ranking_last_or = 1 and c.status = 'delivered' then c.company_name end) not in ('Lidl', 'Carrefour', 'Makro') as last_with_markup,
		max(case when c.express then 1 else 0 end) = 1 as express,
		max(
			case
				when c.ranking_last_or <> 1 then 0
				when c.status <> 'delivered' then 0
				when not c.otd then 1
				when c.has_reschedules then 1
				else 0
				end
			) = 1 as last_not_otd,
		max(case when c.ranking_last_or_all = 1 and c.status = 'canceled' then 1 else 0 end) = 1 as last_canceled,
		max(
			case
				when c.ranking_last_or <> 1 then 0
				when c.status <> 'delivered' then 0
				when c.has_incidence then 1
				when c.has_other_incidence then 1
				when c.has_product_incidence then 1
				else 0
				end
			) = 1 as last_order_incidence
	from
		carts c
			left join
		available_postal_code apc on c.postal_code = apc.postal_code
	group by 1
)
select
	u.id as user_id,
	cpu.service_available,
	cpu.city,
	cpu.postal_code,
	cpu.ors,
	date_diff('seconds', '1970-01-01', cpu.first_date_delivered::date) * 1000 as first_order_date,
	date_diff('seconds', '1970-01-01', cpu.last_date_delivered::date) * 1000 as last_order_date,
	date_diff('seconds', '1970-01-01', cpu.last_deal_date::date) * 1000 as last_deal_date,
	cpu.price,
	cpu.discount,
	cpu.gmv,
	cpu.device_method,
	cpu.avg_cart,
	cpu.coupon_code,
	cpu.last_rating,
	u.phone,
	u.email,
	u.email_subscribed,
	u.email_verified,
	date_diff('seconds', '1970-01-01', date_add('seconds', u.date_joined, '1970-01-01')::date) * 1000 as date_joined,
	trim(coalesce(u.name, '') || ' ' || coalesce(u.surname, '')) as full_name,
	u.locale,
	coalesce(coupon.pending, 0) as wallet,
	coupon.currency as wallet_currency,
	coupon.code as member_get_member_code,
	case
		when cpu.last_date_delivered is null then 999
		when datediff('day', cpu.last_date_delivered, getdate()::date) > 100 then 999
		else datediff('day', cpu.last_date_delivered, getdate()::date)
	end as days_without_ors,
	cpu.multitienda,
	cpu.last_with_markup,
	cpu.express,
	cpu.last_not_otd,
	cpu.last_canceled,
	cpu.last_order_incidence,
	date_diff('seconds', '1970-01-01', mgm.dt_mgm) * 1000 as date_mgm,
	ratings_5.user_id is not null as last_rating_5,
	date_diff('seconds', '1970-01-01', ratings_5.last_rating) * 1000 as date_last_rating
from
	comprea.user u
		left join
	carts_per_user cpu on u.id = cpu.user_id
		left join
	comprea.coupon on cpu.user_id = coupon.user_id and coupon.type = 'person'
		left join
	mgm on u.id = mgm.user_id
		left join
	ratings_5 on u.id = ratings_5.user_id
where
	coalesce(u.email, u.phone) is not null
	/*
	and (
		cpu.last_deal_date >= '2017-01-01'
		or date_add('seconds', u.date_registered, '1970-01-01')::date >= '2017-01-01'
		or mgm.dt_mgm is not null
	)
	*/
"""

HUBSPOT_USERS_DATA_INCREMENTAL = """
	and (
		cpu.last_deal_date >= date_trunc('hour', getdate() - interval '6 hour')
		or cpu.last_date_delivered >= date_trunc('hour', getdate() - interval '6 hour')
		or date_add('seconds', u.date_registered, '1970-01-01') >= date_trunc('hour', getdate() - interval '6 hour')
		or date_add('seconds', u.last_access , '1970-01-01') >= date_trunc('hour', getdate() - interval '6 hour')
		or mgm.dt_mgm >= date_trunc('hour', getdate() - interval '6 hour')
	)
"""
		# cpu.last_deal_date >= date_trunc('hour', getdate() - interval '20 day')
		# or cpu.last_date_delivered >= date_trunc('hour', getdate() - interval '20 day')
		# or date_add('seconds', u.date_registered, '1970-01-01') >= date_trunc('hour', getdate() - interval '20 day')

HUBSPOT_USERS_DATA_INCREMENTAL_DAY = """
	and (
		cpu.last_deal_date >= getdate()::date - interval '1 day'
		or cpu.last_date_delivered >= getdate()::date - interval '1 day'
		or date_add('seconds', u.date_registered, '1970-01-01')::date >= getdate()::date - interval '1 day'
		or date_add('seconds', u.last_access , '1970-01-01') >= date_trunc('hour', getdate() - interval '1 day')
		or mgm.dt_mgm >= date_trunc('hour', getdate() - interval '1 day')
	)
"""

HUBSPOT_USERS_DATA_30_DAY = """
	and (
		cpu.last_deal_date >= getdate()::date - interval '30 day'
		or cpu.last_date_delivered >= getdate()::date - interval '30 day'
		or date_add('seconds', u.date_registered, '1970-01-01')::date >= getdate()::date - interval '30 day'
		or date_add('seconds', u.last_access , '1970-01-01') >= date_trunc('hour', getdate() - interval '30 day')
		or mgm.dt_mgm >= date_trunc('hour', getdate() - interval '30 day')
	)
"""

HUBSPOT_CARTS_DATA ="""
with
coupon_code as (
	select
		cc.cart_id,
		max(c.code) as code
	from
		comprea.cart_coupon cc
			inner join
		comprea.coupon c on cc.coupon_id = c.id
	group by 1
),
city_country as (
	select
		city,
		min(case
				when city = 'Bucharest' then 'Romania'
				else replace(country, 'España', 'Spain')
			end
		) as country
	from
		comprea.postalcode
	where
		country is not null
		and city is not null
		and country <> 'Singapur'
		and country <> 'Estados Unidos'
	group by 1
)
select
	c.id as cart_id,
	co.user_id,
	case
		when coalesce(p.country, ccountry.country, '') = 'Romania' then right((1000000 + p.value::int)::varchar, 6)
		when coalesce(p.country, ccountry.country, '') <> 'España' and coalesce(p.country, ccountry.country, '') <> 'Spain' then p.value
		when p.value = '' then null
		when REGEXP_COUNT(p.value, '[0-9][0-9][0-9][0-9]') = 0 then null
		when REGEXP_COUNT(lower(p.value), '[a-z]') > 0 then null
		when p.value::int < 1000 then null
		when p.value::int > 99999 then null
		else right((100000 + p.value::int)::varchar, 5)
	end as postal_code,
	co.currency,
	co.device_method,
	c.status,
	p.city,
	coalesce(c.num_products_taken, c.num_products) as products,
	comp."name" as company,
	cc.code as coupon_code,
	coalesce(c.shopper_total_price, c.total_price, 0) + 
		coalesce(co.delivery_price)
	as price,
	coalesce(c.total_price_discount, 0) +
		coalesce(co.delivery_price_discount)
	as discount,
	coalesce(c.shopper_total_price, c.total_price, 0) + 
		coalesce(co.delivery_price) -
		coalesce(c.total_price_discount, 0) -
		coalesce(co.delivery_price_discount)
	as gmv,
	date_diff('seconds', '1970-01-01', date_add('seconds', c.last_update, '1970-01-01')::date) * 1000 as last_update,
	sb.rating,
	date_diff('seconds', '1970-01-01', date_add('seconds', c.date_delivered, '1970-01-01')::date) * 1000 as delivered_date,
	u.email as user_email
from
	comprea.cart c
		inner join
	comprea.cash_order co on c.cash_order_id = co.id
		inner join
	comprea.user u on co.user_id = u.id
		left join
	comprea.shopper_timeslot st on c.shopper_timeslot_id = st.id
		left join
	comprea.shopper_bill sb on c.id = sb.cart_id and st.shopper_id = sb.shopper_id
		left join
	comprea.address a on c.address_id = a.id
		left join
	comprea.postalcode p on a.postalcode_id = p.id
		left join
	city_country ccountry on p.city = ccountry.city
		left join
	comprea.shop s on c.shop_id = s.id
		left join
	comprea.company comp on s.company_id = comp.id
		left join
	coupon_code cc on c.id = cc.cart_id
where
	c.status = 'delivered'
	and coalesce(u.email, u.phone) is not null
"""

HUBSPOT_CARTS_DATA_INCREMENTAL = """
	and (
		date_add('seconds', c.last_update, '1970-01-01') >= date_trunc('hour', getdate() - interval '1 hour')
		or date_add('seconds', c.date_delivered, '1970-01-01') >= date_trunc('hour', getdate() - interval '1 hour')
	)
	"""

HUBSPOT_CARTS_DATA_INCREMENTAL_DAY = """
	and (
		date_add('seconds', c.last_update, '1970-01-01') >= getdate() - interval '1 day'
		or date_add('seconds', c.date_delivered, '1970-01-01') >= getdate() - interval '1 day'
	)
	"""

HUBSPOT_CARTS_DATA_30_DAY = """
	and (
		date_add('seconds', c.last_update, '1970-01-01') >= getdate() - interval '30 day'
		or date_add('seconds', c.date_delivered, '1970-01-01') >= getdate() - interval '30 day'
	)
	"""

HUBSPOT_CARTS_DATA_CUSTOM = """
	and (
		date_add('seconds', c.last_update, '1970-01-01') >= '2021-02-26'
		or date_add('seconds', c.date_delivered, '1970-01-01') >= '2021-02-26'
	)
	"""

HUBSPOT_ABANDONED_DATA = """
with
products as (
	select
		cp.cart_id,
		cp.total_price,
		cp.quantity,
		p.name,
		'https://d2ohdpvxj0yo9f.cloudfront.net/products/' || left(p2.picture, 2) || '/' || p2.picture as picture,
		row_number() over(partition by cp.cart_id order by date_changed desc) as product_num
	from
		comprea.cart_product cp
			inner join
		comprea.product_shop ps on cp.product_shop_id = ps.id
			inner join
		comprea.product p on ps.product_id = p.id
			inner join
		comprea.picture p2 on p.picture_id = p2.id
			inner join
		comprea.cart c on cp.cart_id = c.id
	where
		date_add('seconds', c.last_update, '1970-01-01')::date >= (CURRENT_DATE - interval '10 days')::date
),
products_per_cart as (
	select
		p.cart_id,
		max(case when product_num = 1 then name else null end) as name_1,
		max(case when product_num = 1 then picture else null end) as picture_1,
		max(case when product_num = 1 then total_price else null end) as total_price_1,
		max(case when product_num = 1 then quantity else null end) as quantity_1,
		max(case when product_num = 2 then name else null end) as name_2,
		max(case when product_num = 2 then picture else null end) as picture_2,
		max(case when product_num = 2 then total_price else null end) as total_price_2,
		max(case when product_num = 2 then quantity else null end) as quantity_2,
		max(case when product_num = 3 then name else null end) as name_3,
		max(case when product_num = 3 then picture else null end) as picture_3,
		max(case when product_num = 3 then total_price else null end) as total_price_3,
		max(case when product_num = 3 then quantity else null end) as quantity_3,
		max(case when product_num = 4 then name else null end) as name_4,
		max(case when product_num = 4 then picture else null end) as picture_4,
		max(case when product_num = 4 then total_price else null end) as total_price_4,
		max(case when product_num = 4 then quantity else null end) as quantity_4,
		max(case when product_num = 5 then name else null end) as name_5,
		max(case when product_num = 5 then picture else null end) as picture_5,
		max(case when product_num = 5 then total_price else null end) as total_price_5,
		max(case when product_num = 5 then quantity else null end) as quantity_5,
		max(case when product_num = 6 then name else null end) as name_6,
		max(case when product_num = 6 then picture else null end) as picture_6,
		max(case when product_num = 6 then total_price else null end) as total_price_6,
		max(case when product_num = 6 then quantity else null end) as quantity_6,
		max(case when product_num = 7 then name else null end) as name_7,
		max(case when product_num = 7 then picture else null end) as picture_7,
		max(case when product_num = 7 then total_price else null end) as total_price_7,
		max(case when product_num = 7 then quantity else null end) as quantity_7,
		max(case when product_num = 8 then name else null end) as name_8,
		max(case when product_num = 8 then picture else null end) as picture_8,
		max(case when product_num = 8 then total_price else null end) as total_price_8,
		max(case when product_num = 8 then quantity else null end) as quantity_8,
		count(*) as total_products
	from
		products p
	group by 1
),
user_carts as (
	select
		date_add('seconds', c.last_update, '1970-01-01') as last_update,
		date_add('seconds', c.date_started , '1970-01-01') as date_started,
		co.user_id,
		u.email,
		c.status,
		ppc.*,
		rank() over(partition by co.user_id order by c.last_update desc, ppc.total_products desc, c.id desc) as user_ranking
	from
		comprea.cart c
			inner join
		comprea.cash_order co on c.cash_order_id = co.id
			inner join
		comprea.user u on co.user_id = u.id
			inner join
		products_per_cart ppc on c.id = ppc.cart_id
)
select
	last_update,
	date_started,
	user_id,
	email,
	name_1,
	picture_1,
	total_price_1,
	quantity_1,
	name_2,
	picture_2,
	total_price_2,
	quantity_2,
	name_3,
	picture_3,
	total_price_3,
	quantity_3,
	name_4,
	picture_4,
	total_price_4,
	quantity_4,
	name_5,
	picture_5,
	total_price_5,
	quantity_5,
	name_6,
	picture_6,
	total_price_6,
	quantity_6,
	name_7,
	picture_7,
	total_price_7,
	quantity_7,
	name_8,
	picture_8,
	total_price_8,
	quantity_8,
	total_products
from
	user_carts
where
	user_ranking = 1
	and status = 'started'
	and date_trunc('hour', last_update) >= date_trunc('hour', getdate() - interval '2 days')
"""

HUBSPOT_ABANDONED_DATA_INCREMENTAL = """
	and date_trunc('hour', last_update) >= date_trunc('hour', getdate() - interval '24 hours')
"""


AIRCALL_DELETE_LOADED_RECORDS = """
delete from
	bi_development.aircall_calls
where
	exists (select *
				from bi_development_stg.v_aircall_calls_stg acs
				where aircall_calls.id = acs.id
			)
"""

AIRCALL_LOAD_NEW_RECORDS = """
insert into bi_development.aircall_calls
select
	*
from
	bi_development_stg.v_aircall_calls_stg acs
where
	not exists (select *
					from bi_development.aircall_calls ac
					where ac.id = acs.id
				)
"""


HUBSPOT_LOAD_CAMPAIGNS = """
insert into bi_development.hubspot_campaigns
select
	*
from
	bi_development_stg.v_hubspot_campaigns_stg hcs
where
	not exists(select *
				from bi_development.hubspot_campaigns hc
				where hcs.email_campaign_id = hc.email_campaign_id
			)
"""

HUBSPOT_LOAD_EVENTS = """
insert into bi_development.hubspot_events
select
	*
from
	bi_development_stg.v_hubspot_events_stg hes
where
	not exists (select *
				from
					bi_development.hubspot_events he
				where
					hes.created = he.created
					and hes.recipient = he.recipient
					and hes.type = he.type
					and hes.email_campaign_id = he.email_campaign_id
				)
"""


HELPSCOUT_INSERT = """
insert into bi_development.helpscout_conversations
select
	*
from
	bi_development_stg.v_helpscout_conversations_stg v_hs
where
	not exists (select * from bi_development.helpscout_conversations hsc where hsc.id = v_hs.id)
"""

HELPSCOUT_DELETE = """
delete
from
	bi_development.helpscout_conversations
where
	exists (select * from bi_development_stg.helpscout_conversations_stg hs_stg where helpscout_conversations.id = hs_stg.id::int)
"""

HELPSCOUT_CLOSE = """
update
	bi_development.helpscout_conversations
set
	status = 'closed'
where
	status ='active'
	and customer_waiting_since_time >= CURRENT_DATE - INTERVAL '{days} day'
"""

HELPSCOUT_ACTIVE = """
insert into bi_development.helpscout_conversations_active
select
	(CURRENT_TIMESTAMP - interval '20 hour')::date as active_date,
	id
from
	bi_development.helpscout_conversations hc
where
	status in ('active', 'pending')
	and not exists (select *
					from bi_development.helpscout_conversations_active hca
					where
						hc.id = hca.id
						and (CURRENT_TIMESTAMP - interval '20 hour')::date = hca.active_date
					)
"""

DELIGHTED_SURVEYS_CHANGES = """
insert into bi_development.delighted_surveys_changes
select
	ds.id,
	ds.score as score_old,
	dss.score as score_new,
	ds.comment as comment_old,
	dss.comment as comment_new,
	ds.additional_answers_question as additional_answers_question_old,
	dss.additional_answers_question as additional_answers_question_new,
	ds.additional_answers_text as additional_answers_text_old,
	dss.additional_answers_text as additional_answers_text_new,
	ds.updated_at as updated_at_old,
	dss.updated_at as updated_at_new
from
	bi_development.delighted_surveys ds
		inner join
	bi_development_stg.v_delighted_surveys_stg dss
		on
			ds.id = dss.id
			and ds.survey_type = dss.survey_type
where
	ds.updated_at <> dss.updated_at
"""

DELIGHTED_SURVEYS_DELETE = """
delete
from
	bi_development.delighted_surveys
where
	exists(select *
			from bi_development_stg.v_delighted_surveys_stg dss
			where
				dss.id = delighted_surveys.id
				and dss.survey_type = delighted_surveys.survey_type
				and dss.updated_at <> delighted_surveys.updated_at
		)
"""

DELIGHTED_SURVEYS = """
insert into bi_development.delighted_surveys
select * from bi_development_stg.v_delighted_surveys_stg dss
where not exists (select * from bi_development.delighted_surveys ds where dss.id = ds.id and dss.survey_type = ds.survey_type)
"""

DELIGHTED_PEOPLE = """
insert into bi_development.delighted_people
select * from bi_development_stg.v_delighted_people_stg dss
where not exists (select * from bi_development.delighted_people ds where dss.id = ds.id)
"""