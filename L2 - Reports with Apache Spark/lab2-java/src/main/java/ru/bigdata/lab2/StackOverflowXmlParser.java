package ru.bigdata.lab2;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.InputSource;
import org.xml.sax.SAXParseException;
import org.xml.sax.helpers.DefaultHandler;
import ru.bigdata.lab2.model.StackOverflowPost;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.StringReader;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;

public final class StackOverflowXmlParser {

    private static final ThreadLocal<DocumentBuilder> DOCUMENT_BUILDER = ThreadLocal.withInitial(() -> {
        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setNamespaceAware(false);
            factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
            factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
            factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
            factory.setXIncludeAware(false);
            factory.setExpandEntityReferences(false);
            DocumentBuilder builder = factory.newDocumentBuilder();
            builder.setErrorHandler(new DefaultHandler() {
                @Override
                public void error(SAXParseException exception) {
                }

                @Override
                public void fatalError(SAXParseException exception) {
                }
            });
            return builder;
        } catch (Exception exception) {
            throw new IllegalStateException("Cannot initialize XML parser", exception);
        }
    });

    private StackOverflowXmlParser() {
    }

    public static StackOverflowPost parse(String xmlRow) {
        try {
            DocumentBuilder builder = DOCUMENT_BUILDER.get();
            Document document = builder.parse(new InputSource(new StringReader(xmlRow)));
            Element row = document.getDocumentElement();

            String id = row.getAttribute("Id");
            String postTypeId = row.getAttribute("PostTypeId");
            String creationDate = row.getAttribute("CreationDate");
            if (id.isBlank() || postTypeId.isBlank() || creationDate.isBlank()) {
                return null;
            }

            StackOverflowPost post = new StackOverflowPost();
            post.setId(Long.parseLong(id));
            post.setPostTypeId(Integer.parseInt(postTypeId));
            post.setCreationDate(LocalDateTime.parse(creationDate));
            post.setYear(post.getCreationDate().getYear());
            post.setTitle(row.getAttribute("Title"));
            post.setTags(row.getAttribute("Tags"));
            return post;
        } catch (DateTimeParseException | NumberFormatException exception) {
            return null;
        } catch (Exception exception) {
            return null;
        }
    }
}
