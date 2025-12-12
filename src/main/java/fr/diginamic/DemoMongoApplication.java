package fr.diginamic;

import com.mongodb.client.result.DeleteResult;
import org.bson.Document;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Projections.*;

public class DemoMongoApplication {

  public static void main(String[] args) throws IOException {
    Properties props = new Properties();
    InputStream input = DemoMongoApplication.class
      .getClassLoader()
      .getResourceAsStream("application.properties");
    props.load(input);

    String host = props.getProperty("mongodb.host");
    String dbName = props.getProperty("mongodb.database");
    String collName = props.getProperty("mongodb.collection");


    String uri = "mongodb://" + host + "/";


    MongoManager mongoManager = new MongoManager(uri, dbName, collName);

    System.out.println("Connecté à ... ");
    System.out.println("Database :" + mongoManager.getDatabase().getName() + "\nCollection :" + mongoManager.getCollection().getNamespace());

    // A - 1 - Insérez de nouveaux fruits et légumes dans la collection products.
    createFruitsAndVegetable(mongoManager);

    String s = "=";
    System.out.println("\n" + s.repeat(100));

    // B - 1 - Mettez à jour le prix et la quantité d'un produit spécifique.
    System.out.println("Watermelon avant :\n");
    Document query = new Document("name", "Watermelon");
    Document result = mongoManager.readOneDocument(query);
    showDetailsOfDocument(result);

    Document update = new Document("$set", new Document("price", 2.3).append("quantity", 20));
    mongoManager.updateOneDocument(query, update);

    System.out.println("Watermelon après :\n");
    result = mongoManager.readOneDocument(query);
    showDetailsOfDocument(result);

    // B - 2 - Ajoutez une nouvelle propriété à un produit existant.
    System.out.println("Ajout de la propriété weight à la clementine :\n");
    Document queryClementine = new Document("name", "Clementine");
    mongoManager.getCollection().updateOne(queryClementine, new Document("$set", new Document("weight", 0.2)));
    result = mongoManager.readOneDocument(queryClementine);
    showDetailsOfDocument(result);

    // B - 3 - Supprimez une propriété d'un produit existant.
    System.out.println("Suppression de la propriété couleur de la clementine :\n");
    mongoManager.getCollection().updateOne(queryClementine, new Document("$unset", new Document("color", "")));
    result = mongoManager.readOneDocument(queryClementine);
    showDetailsOfDocument(result);

    System.out.println("\n" + s.repeat(100));

    // C - 1 - Ajoutez un élément à un tableau. Exemple : 'alternative_colors' égal à 'Green'.
    System.out.println("ajout de tags :\n");
    mongoManager.getCollection().updateMany(new Document(), new Document("$set", new Document("tags", new ArrayList<String>())));
    mongoManager.getCollection().updateOne(queryClementine, new Document("$push", new Document("tags", "citric")));
    // C - 3 - Supprimez un élément d'un tableau.
    mongoManager.getCollection().updateOne(queryClementine, new Document("$pull", new Document("tags", "citric")));
    // C - 2 - Ajoutez plusieurs éléments à un tableau.
    mongoManager.getCollection().updateOne(queryClementine, new Document("$set", new Document("tags", Arrays.asList("fruit", "breakfast"))));
    // C - 4 - Supprimez le dernier élément d'un tableau.
    mongoManager.getCollection().updateOne(queryClementine, new Document("$pop", new Document("tags", 1)));
    result = mongoManager.readOneDocument(queryClementine);
    showDetailsOfDocument(result);
    mongoManager.getCollection().updateMany(new Document(), new Document("$unset", new Document("tags", "")));

    System.out.println("\n" + s.repeat(100));

    // D - 1 - Supprimez un fruit spécifique grâce à son _id de la collection products.
    Document queryProduct = new Document("name", "Clementine");
    Document clementine = mongoManager.getCollection().find(queryProduct).first();
    assert clementine != null;
    query = new Document("_id", clementine.getObjectId("_id"));
    mongoManager.getCollection().deleteOne(query);

    System.out.println("Clementine supprimée.");

    // D - 2 - Supprimer tous les fruits et légumes qui sont Green.
    Document greenVeg = new Document()
      .append("name", "Broccoli")
      .append("price", 2.10)
      .append("quantity", 3)
      .append("category", "Vegetable")
      .append("color", "Green");

    mongoManager.createOneDocument(greenVeg);

    // suppression
    Document queryProductGreen = new Document("color", "Green");
    DeleteResult res = mongoManager.getCollection().deleteMany(queryProductGreen);
    System.out.println("Documents supprimés : " + res.getDeletedCount());

    System.out.println("\n" + s.repeat(100));


    // E - 1 - Recherchez tous les produits de couleur rouge.
    System.out.println("Tout les documents rouge :\n");
    List<Document> allDocumentsRed = mongoManager.readManyDocuments(new Document("color", "Red"));
    showDetailsOfDocuments(allDocumentsRed);

    // E - 2 -  Recherchez tous les produits dont le prix est inférieur à 2.00.
    System.out.println("Tout les produits dont le prix est inférieur à 2.00 :\n");
    List<Document> allDocumentsInf = mongoManager.readManyDocuments(new Document("price", new Document("$lt", 2)));
    showDetailsOfDocuments(allDocumentsInf);

    // E - 3 - Recherchez le fruit qui à la plus grande quantité.
    System.out.println("le fruit qui à la plus grande quantité :\n");
    List<Document> allDocumentsQuantity = new ArrayList<>();
    mongoManager.getCollection().find().sort(new Document("quantity", -1)).limit(1).into(allDocumentsQuantity);
    showDetailsOfDocuments(allDocumentsQuantity);


    // -------------------------------------------------------------------------------------

    mongoManager.closeConnection();
  }


  public static void createFruitsAndVegetable(MongoManager mongoManager) {
    // creation de nos documents
    Document fruit1 = new Document().append("name", "Clementine").append("price", 1.05).append("quantity", 12).append("category", "Fruit").append("color", "Orange");
    Document fruit2 = new Document().append("name", "Mandarine").append("price", 1.25).append("quantity", 5).append("category", "Fruit").append("color", "Orange");
    Document vegetable1 = new Document().append("name", "sweet Potato").append("price", 3.05).append("quantity", 2).append("category", "Vegetable");
    List<Document> vegetablesAndFruits = Arrays.asList(fruit1, fruit2, vegetable1);

    // insertion
    mongoManager.createManyDocuments(vegetablesAndFruits);

    List<Document> vegetablesAndFruits2 =
      mongoManager.getCollection()
        .find(in("name", "Clementine", "Mandarine", "Sweet Potato"))
        .projection(fields(include("name", "category"), excludeId()))
        .into(new ArrayList<>());

    showDetailsOfDocuments(vegetablesAndFruits);

  }

  public static void showDetailsOfDocuments(List<Document> docs) {
    // affichage
    for (Document doc : docs) {
      showDetailsOfDocument(doc);
    }
  }

  public static void showDetailsOfDocument(Document doc) {
    System.out.println(doc.toJson());
  }


}

