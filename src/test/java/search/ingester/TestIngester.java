
package search.ingester;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.List;

 
public class TestIngester {
 
    @Test
    public void exampleAssertion() {
        assertTrue(3 == 1 + 1 + 1, "Can add up.");
    }

    @Test
    public void exampleMock() {
        // mock creation
        List<Integer> mockedList = mock(ArrayList.class);

        mockedList.add(3);
        mockedList.clear();

        verify(mockedList).add(3);
        verify(mockedList).clear();
    }

    @Test
    public void shouldDispatchVerbToTheRightHandler() {
        assertTrue(3 == 1 + 1 + 1, "Can add up.");
    }    
}
