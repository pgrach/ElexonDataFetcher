import { useState } from 'react';
import { MapContainer, TileLayer, Circle, Popup } from 'react-leaflet';
import { scaleLinear } from 'd3-scale';
import { useQuery } from '@tanstack/react-query';
import { Link } from 'wouter';
import { Button } from '@/components/ui/button';
import { ArrowLeft } from 'lucide-react';
import 'leaflet/dist/leaflet.css';

// Center of the UK for initial map view
const UK_CENTER = [55.378051, -3.435973];
const INITIAL_ZOOM = 6;

// Color scale for the heatmap
const colorScale = scaleLinear<string>()
  .domain([0, 50, 100])
  .range(['#00ff00', '#ffff00', '#ff0000']);

interface WindFarmPerformance {
  farmId: string;
  name: string;
  latitude: number;
  longitude: number;
  curtailedEnergy: number;
  payment: number;
  capacity: number;
  utilizationRate: number;
}

export default function HeatmapPage() {
  const [selectedDate, setSelectedDate] = useState<string>(
    new Date().toISOString().split('T')[0]
  );

  const { data: windFarms, isLoading } = useQuery<WindFarmPerformance[]>({
    queryKey: ['/api/wind-farms/performance', selectedDate],
    enabled: !!selectedDate,
  });

  return (
    <div className="min-h-screen flex flex-col">
      <div className="p-4 bg-card border-b">
        <div className="container mx-auto flex justify-between items-center">
          <div className="flex items-center gap-4">
            <Link href="/">
              <Button variant="ghost" size="icon">
                <ArrowLeft className="h-4 w-4" />
              </Button>
            </Link>
            <h1 className="text-2xl font-bold">Wind Farm Performance Heatmap</h1>
          </div>
          <input
            type="date"
            value={selectedDate}
            onChange={(e) => setSelectedDate(e.target.value)}
            className="border rounded p-2"
          />
        </div>
      </div>

      <div className="flex-1 relative">
        {isLoading ? (
          <div className="absolute inset-0 flex items-center justify-center bg-background/80 z-50">
            <div className="text-lg">Loading map data...</div>
          </div>
        ) : (
          <MapContainer
            center={UK_CENTER}
            zoom={INITIAL_ZOOM}
            className="h-[calc(100vh-4rem)] w-full"
            style={{ background: '#f0f0f0' }}
          >
            <TileLayer
              attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
              url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
            />

            {windFarms?.map((farm) => (
              <Circle
                key={farm.farmId}
                center={[farm.latitude, farm.longitude]}
                radius={5000}
                pathOptions={{
                  color: colorScale(farm.utilizationRate),
                  fillColor: colorScale(farm.utilizationRate),
                  fillOpacity: 0.6,
                }}
              >
                <Popup>
                  <div className="p-2">
                    <h3 className="font-bold">{farm.name}</h3>
                    <p>Curtailed Energy: {farm.curtailedEnergy.toFixed(2)} MWh</p>
                    <p>Payment: £{farm.payment.toFixed(2)}</p>
                    <p>Capacity: {farm.capacity} MW</p>
                    <p>Utilization: {farm.utilizationRate.toFixed(1)}%</p>
                  </div>
                </Popup>
              </Circle>
            ))}
          </MapContainer>
        )}
      </div>
    </div>
  );
}