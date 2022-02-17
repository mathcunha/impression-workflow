class Impression:
    def __init__(self, app_id, country_code, impressions, clicks, revenue):
        self.app_id = app_id
        self.country_code = country_code
        self.impressions = impressions
        self.clicks = clicks
        self.revenue = revenue


class Revenue:
    def __init__(self, app_id, country_code, recommended_advertiser_ids):
        self.app_id = app_id
        self.country_code = country_code
        self.recommended_advertiser_ids = list(map(int, recommended_advertiser_ids.split(','))) if recommended_advertiser_ids else []