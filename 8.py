import asyncio
from pyle38 import Tile38
from typing import List, Tuple


class Tile38Client:
    def __init__(self, host: str = 'localhost', port: int = 9851):
        """Initialize Tile38 client connection"""
        self.client = Tile38(url=f"redis://{host}:{port}")

    async def basic_operations(self):
        """Basic operations with geospatial data"""
        # Add point objects (couriers)
        await self.client.set("fleet", "courier1").point(52.25, 13.37).exec()
        await self.client.set("fleet", "courier2").point(40.7128,
                                                         -74.0060).exec()

        # Add object with additional fields
        courier_data = {
            'name': 'John Doe',
            'vehicle': 'bike',
            'status': 'active'
        }

        await self.client.set("fleet", "courier3").point(51.5074,
                                                         -0.1278).fields(
            courier_data).exec()

        # Get object information
        return await self.client.get("fleet", "courier1").asObject()

    async def geofencing(self):
        """Example of working with geofences"""
        # Create a geofence for city center using circle
        await self.client.set("zones", "city_center").object({
            "type": "Polygon",
            "coordinates": [[
                [13.37, 52.25],
                [13.38, 52.25],
                [13.38, 52.26],
                [13.37, 52.26],
                [13.37, 52.25]
            ]]
        }).exec()

        # Check if any couriers are inside the zone using WITHIN
        inside_zone = await self.client.within("fleet").get("zones",
                                                            "city_center").asObjects()

        # Setup webhook for geofence notifications
        hook = await self.client.sethook(
            "city_alerts",
            "http://example.com/webhook"
        ).within("fleet").get("zones", "city_center").detect(
            ["enter", "exit"]).activate()

        return inside_zone

    async def proximity_search(self):
        """Search for nearest objects"""
        # Add some POIs
        pois = {
            'restaurant1': (
            52.25, 13.37, {'name': 'Pizza Place', 'type': 'restaurant'}),
            'restaurant2': (
            52.26, 13.37, {'name': 'Sushi Bar', 'type': 'restaurant'}),
            'restaurant3': (
            52.25, 13.38, {'name': 'Burger Joint', 'type': 'restaurant'})
        }

        for id, (lat, lon, data) in pois.items():
            await self.client.set('pois', id).point(lat, lon).fields(
                data).exec()

        # Search POIs within 1000 meters
        search_point = (52.25, 13.37)
        nearby = await self.client.nearby('pois').point(
            search_point[0],
            search_point[1],
            1000
        ).asObjects()

        return nearby

    async def routing(self):
        """Delivery routing example"""

        async def update_courier_position(courier_id: str,
                                          route: List[Tuple[float, float]]):
            """Update courier position along route"""
            for lat, lon in route:
                await self.client.set('fleet', courier_id).point(lat,
                                                                 lon).exec()

        route = [
            (52.25, 13.37),
            (52.26, 13.37),
            (52.25, 13.38)
        ]

        await update_courier_position('courier1', route)
        return await self.client.scan('fleet').asObjects()


async def main():
    # Initialize client
    tile38 = Tile38Client()

    try:

        # Run examples
        basic_result = await tile38.basic_operations()
        print("Basic operations result:", basic_result)

        geofence_result = await tile38.geofencing()
        print("Geofencing result:", geofence_result)

        nearby_places = await tile38.proximity_search()
        print("Nearby places:", nearby_places)

        route_history = await tile38.routing()
        print("Route history:", route_history)

    except Exception as e:
        print(f"Error occurred: {e}")
    finally:
        await tile38.client.quit()


if __name__ == "__main__":
    asyncio.run(main())