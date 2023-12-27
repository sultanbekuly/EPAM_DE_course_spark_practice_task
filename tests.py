import unittest
from unittest.mock import patch
from main import generate_geohash, geocode_restaurant


class TestGenerateGeohash(unittest.TestCase):

    def test_generate_geohash_valid(self):
        lat, lng = 40.7128, -74.0060  # Example coordinates for New York City
        expected_geohash = 'dr5r'  # Expected geohash
        self.assertEqual(generate_geohash(lat, lng), expected_geohash)

    def test_generate_geohash_invalid(self):
        self.assertIsNone(generate_geohash(None, None))


class TestGeocodeRestaurant(unittest.TestCase):

    @patch('main.OpenCage')
    def test_geocode_restaurant_success(self, mock_geocoder):
        # Mock the geocoder response for a successful geocoding
        mock_geocoder.return_value.geocode.return_value = MockLocation(40.7128, -74.0060)

        api_key = "fake_api_key"
        result = geocode_restaurant("Some Restaurant", "New York", "US", api_key)
        self.assertEqual(result, (40.7128, -74.0060))

    @patch('main.OpenCage')
    def test_geocode_restaurant_failure(self, mock_geocoder):
        # Mock the geocoder response for a failed geocoding
        mock_geocoder.return_value.geocode.return_value = None

        api_key = "fake_api_key"
        result = geocode_restaurant("the house from 'Courage the Cowardly Dog'", "Nowhere", "Narnia", api_key)
        self.assertEqual(result, (None, None))


# A mock location class to simulate geocoder response
class MockLocation:
    def __init__(self, latitude, longitude):
        self.latitude = latitude
        self.longitude = longitude

    def geocode(self, query, country=None):
        return self


if __name__ == 'main':
    unittest.main()
