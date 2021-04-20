package br.ufrpe.vgf;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.fasterxml.jackson.databind.ObjectMapper;

import br.ufrpe.vgf.model.Pedido;

public class PedidosLambda {
	private final DynamoDB dynamoDB = new DynamoDB(AmazonDynamoDBClientBuilder.defaultClient());
	private final ObjectMapper objectMapper = new ObjectMapper();
	private final String tableNameOnline = "PedidosOnline";
	private final String tableNameFisico = "PedidosFisicos";

	public String handleRequest(KinesisEvent event, Context context) {

		Table tabelaOnline = dynamoDB.getTable(tableNameOnline);
		Table tabelaFisica = dynamoDB.getTable(tableNameFisico);
		for (KinesisEvent.KinesisEventRecord record : event.getRecords()) {
			String data = StandardCharsets.UTF_8.decode(record.getKinesis().getData()).toString();
			Pedido order = null;
			try {
				order = objectMapper.readValue(data, Pedido.class);
			} catch (IOException e) {
				e.printStackTrace();
			}

			if (order.getTipoVenda().equals("Venda Online")) {
				Item item = new Item().withPrimaryKey("pedidoId", order.getPedidoId())
						.withString("produto", order.getProduto())
						.withInt("quantidade", order.getQuantidade())
						.withString("tipoVenda", order.getTipoVenda());
				tabelaOnline.putItem(item);
			} else {
				Item item = new Item().withPrimaryKey("pedidoId", order.getPedidoId())
						.withString("produto", order.getProduto())
						.withInt("quantidade", order.getQuantidade())
						.withString("tipoVenda", order.getTipoVenda());
				tabelaFisica.putItem(item);
			}

		}

		return "SUCCESS";
	}
}