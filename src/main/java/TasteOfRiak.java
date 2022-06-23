import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.kv.DeleteValue;
import com.basho.riak.client.api.commands.kv.FetchValue;
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.api.commands.kv.UpdateValue;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakNode;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.util.BinaryValue;

import java.net.UnknownHostException;

public class TasteOfRiak {

    public static class Vehicle {
        public String brand;
        public String model;
        public String engine;
        public String series;
    }

    public static class VehicleUpdate extends UpdateValue.Update<Vehicle> {
        private final Vehicle update;
        public VehicleUpdate(Vehicle update){
            this.update = update;
        }

        @Override
        public Vehicle apply(Vehicle t) {
            if(t == null) {
                t = new Vehicle();
            }

            t.brand = update.brand;
            t.model = update.model;
            t.engine = update.engine;
            t.series = update.series;

            return t;
        }
    }
    private static RiakCluster setUpCluster() throws UnknownHostException {
        RiakNode node = new RiakNode.Builder()
                .withRemoteAddress("172.17.0.2")
                .withRemotePort(10017)
                .build();
        RiakCluster cluster = new RiakCluster.Builder(node)
                .build();
        cluster.start();

        return cluster;
    }

    public static void main( String[] args ) {
        try {

            RiakObject quoteObject = new RiakObject()
                    .setContentType("text/plain")
                    .setValue(BinaryValue.create("Samochody"));
            System.out.println("Basic object created");


            Namespace quotesBucket = new Namespace("vehicles");

            Location quoteObjectLocation = new Location(quotesBucket, "Moj ulubiony samochod");
            System.out.println("Location object created for quote object");


            StoreValue storeOp = new StoreValue.Builder(quoteObject)
                    .withLocation(quoteObjectLocation)
                    .build();
            System.out.println("StoreValue operation created");


            RiakCluster cluster = setUpCluster();
            RiakClient client = new RiakClient(cluster);
            System.out.println("Client object successfully created");

            StoreValue.Response storeOpResp = client.execute(storeOp);
            System.out.println("Object storage operation successfully completed");


            FetchValue fetchOp = new FetchValue.Builder(quoteObjectLocation)
                    .build();
            RiakObject fetchedObject = client.execute(fetchOp).getValue(RiakObject.class);
            assert(fetchedObject.getValue().equals(quoteObject.getValue()));
            System.out.println("Success! The object we created and the object we fetched have the same value");


            fetchedObject.setValue(BinaryValue.create("Moj drugi samochod"));
            StoreValue updateOp = new StoreValue.Builder(fetchedObject)
                    .withLocation(quoteObjectLocation)
                    .build();
            StoreValue.Response updateOpResp = client.execute(updateOp);
            updateOpResp = client.execute(updateOp);


            DeleteValue deleteOp = new DeleteValue.Builder(quoteObjectLocation)
                    .build();
            client.execute(deleteOp);
            System.out.println("Quote object successfully deleted");

            Vehicle fiestunia = new Vehicle();
            fiestunia.brand = "Ford";
            fiestunia.model = "Fiesta";
            fiestunia.engine = "Gas";
            fiestunia.series = "Titanium";

            System.out.println("Vehicle object created");

            Namespace vehicleBucket = new Namespace("vehicle");
            Location fiestuniaLocation = new Location(vehicleBucket, "Fiestunia");
            StoreValue storeBookOp = new StoreValue.Builder(fiestunia)
                    .withLocation(fiestuniaLocation)
                    .build();
            client.execute(storeBookOp);
            System.out.println("Fiestunia information now stored in Riak");


            FetchValue fetchFiestuniaOp = new FetchValue.Builder(fiestuniaLocation)
                    .build();
            Vehicle fetchedFiestunia = client.execute(fetchFiestuniaOp).getValue(Vehicle.class);
            System.out.println("Vehicle object successfully fetched");

            assert(fiestunia.getClass() == fetchedFiestunia.getClass());
            assert(fiestunia.brand.equals(fetchedFiestunia.brand));
            assert(fiestunia.model.equals(fetchedFiestunia.model));
            assert(fiestunia.engine.equals(fetchedFiestunia.engine));
            assert(fiestunia.series.equals(fetchedFiestunia.series));

            VehicleUpdate updatedVehicle = new VehicleUpdate(fiestunia);
            UpdateValue updateValue = new UpdateValue.Builder(fiestuniaLocation)
                    .withUpdate(updatedVehicle).build();
            UpdateValue.Response response = client.execute(updateValue);

            System.out.println("Success! All of our tests check out");

            cluster.shutdown();

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}