package ru.bigdata.lab2;

import org.junit.jupiter.api.Test;
import ru.bigdata.lab2.model.StackOverflowPost;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class StackOverflowXmlParserTest {

    @Test
    void parsesQuestionRow() {
        String row = """
                <row Id="4" PostTypeId="1" CreationDate="2010-07-31T21:42:52.667" Title="Convert Decimal to Double?" Tags="&lt;c#&gt;&lt;double&gt;" />
                """.trim();

        StackOverflowPost post = StackOverflowXmlParser.parse(row);

        assertNotNull(post);
        assertEquals(4L, post.getId());
        assertEquals(1, post.getPostTypeId());
        assertEquals(2010, post.getYear());
        assertEquals("Convert Decimal to Double?", post.getTitle());
        assertEquals("<c#><double>", post.getTags());
    }

    @Test
    void returnsNullForBrokenRow() {
        StackOverflowPost post = StackOverflowXmlParser.parse("<row Id=\"broken\"");
        assertNull(post);
    }
}
