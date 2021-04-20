package br.ufrpe.vgf.kld;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import br.ufrpe.vgf.kld.aws.AwsKinesisClient;
import br.ufrpe.vgf.kld.model.Pedido;

public class App {
    List<String> listaDeProdutos = new ArrayList<>();
    List<String> listaTipoDeVendas = new ArrayList<>();

    Random random = new Random();
    public static void main( String[] args ) throws InterruptedException {
        App app = new App();
        app.populateProdutos();
        app.popularTipoVenda();

        AmazonKinesis kinesisClient = AwsKinesisClient.getKinesisClient();
        for(int i=0; i<10; i++){
            app.sendData(kinesisClient);
            Thread.sleep(500);
        }

    }

    private void sendData(AmazonKinesis kinesisClient){
        //2. PutRecordRequest
        PutRecordsRequest recordsRequest = new PutRecordsRequest();
        recordsRequest.setStreamName("pedidos-stream");
        recordsRequest.setRecords(getRecordsRequestList());

        //3. putRecord or putRecords - 500 records with single API call
        PutRecordsResult results = kinesisClient.putRecords(recordsRequest);
        if(results.getFailedRecordCount() > 0){
            System.out.println("Ocorreram erros no registro " + results.getFailedRecordCount());
        } else {
            System.out.println("Dados enviados com sucesso..");
        }

    }

    private List<PutRecordsRequestEntry> getRecordsRequestList(){
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        List<PutRecordsRequestEntry> putRecordsRequestEntries = new ArrayList<>();
        for (Pedido pedido: getPedidos()){
            PutRecordsRequestEntry requestEntry = new PutRecordsRequestEntry();
            requestEntry.setData(ByteBuffer.wrap(gson.toJson(pedido).getBytes()));
            requestEntry.setPartitionKey(UUID.randomUUID().toString());
            putRecordsRequestEntries.add(requestEntry);
        }
        return putRecordsRequestEntries;
    }

    private List<Pedido> getPedidos(){
        List<Pedido> orders = new ArrayList<>();
        for(int i=0;i<1;i++){
            Pedido order = new Pedido();
            order.setPedidoId(Math.abs(random.nextInt()));
            order.setProduto(listaDeProdutos.get(random.nextInt(listaDeProdutos.size())));
            order.setQuantidade(random.nextInt(5));
            order.setTipoVenda(listaTipoDeVendas.get(random.nextInt(listaTipoDeVendas.size())));
            orders.add(order);
        }
        return orders;
    }

    private void populateProdutos(){
        listaDeProdutos.add("Sabre Jedi");
        listaDeProdutos.add("Manto Jedi");
        listaDeProdutos.add("Miniatura do Yoda");
        listaDeProdutos.add("Miniatura R2D2");
        listaDeProdutos.add("Miniatura C3PO");
        listaDeProdutos.add("Pelúcia do Chewbacca");
        listaDeProdutos.add("Armadura Stormtrooper");
    }
    
    private void popularTipoVenda() {
    	listaTipoDeVendas.add("Venda Online");
    	listaTipoDeVendas.add("Recife - Loja Física");
    	listaTipoDeVendas.add("Paulista - Loja Física");
    }


}
