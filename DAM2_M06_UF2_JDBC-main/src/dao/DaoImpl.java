package dao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import model.Card;
import model.Player;
import utils.Color;
import utils.Number;

public class DaoImpl implements Dao {

	private Connection connection;

	// Constantes. Deber�an estar en una clase de constantes en el paquete utils
	public static final String SCHEMA_NAME = "uno";
	public static final String CONNECTION = "jdbc:mysql://localhost:3306/" + SCHEMA_NAME
			+ "?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC";
	public static final String USER_CONNECTION = "root";
	public static final String PASS_CONNECTION = "";
	public static final String GET_PLAYER = "SELECT * FROM player WHERE user = ? AND password = ?";
	public static final String GET_CARD = "SELECT * FROM card LEFT JOIN game ON card.id = game.id_card WHERE id_player = ? AND game.id IS NULL";
	public static final String GET_LASTCARD_DD = "SELECT IFNULL(MAX(id), 0) + 1 FROM card WHERE id_player = ?";
	public static final String SAVE_CARD_DD = "INSERT INTO card (id_player, number, color) VALUES (?, ?, ?)";
	public static final String GET_LASTCARD_COT = "SELECT id_card FROM game WHERE id = (SELECT MAX(id) FROM game)";
	public static final String SAVE_CARD_COT = "SELECT * FROM card WHERE id = ?";
	public static final String ADD_GAMES = "UPDATE player SET games = games + 1 WHERE id = ?";
	public static final String ADD_VICTORIES = "UPDATE player SET victories = victories + 1 WHERE id = ?";
	public static final String CLEAR_DECK = "DELETE FROM card WHERE id_player = ?";
	public static final String DELETE_CARD = "DELETE FROM card WHERE id = ?";
	public static final String SAVE_CARD = "INSERT INTO card (id_player, number, color) VALUES (?, ?, ?)";

	@Override
	public void connect() throws SQLException {
		connection = DriverManager.getConnection(CONNECTION, USER_CONNECTION, PASS_CONNECTION);
	}

	@Override
	public void disconnect() throws SQLException {
		if (connection != null && !connection.isClosed()) {
			connection.close();
		}
	}

	@Override
	public int getLastIdCard(int playerId) throws SQLException {
		connect();
		try (PreparedStatement statement = connection.prepareStatement(GET_LASTCARD_DD)) {
			statement.setInt(1, playerId);
			ResultSet resultSet = statement.executeQuery();
			resultSet.next();
			return resultSet.getInt(1);
		} finally {
			disconnect();
		}
	}

	@Override
	public Card getLastCard() throws SQLException {
		connect();
		try (PreparedStatement statement = connection.prepareStatement(GET_LASTCARD_COT)) {
			ResultSet resultSet = statement.executeQuery();
			resultSet.next();
			int cardId = resultSet.getInt("id_card");
			return getCard(cardId);
		} finally {
			disconnect();
		}
	}

	@Override
	public Player getPlayer(String user, String pass) throws SQLException {
		connect();
		try (PreparedStatement statement = connection.prepareStatement(GET_PLAYER)) {
			statement.setString(1, user);
			statement.setString(2, pass);
			ResultSet resultSet = statement.executeQuery();
			if (resultSet.next()) {
				int id = resultSet.getInt("id");
				String name = resultSet.getString("name");
				int games = resultSet.getInt("games");
				int victories = resultSet.getInt("victories");
				return new Player(id, name, games, victories);
			} else {
				return null;
			}
		} finally {
			disconnect();
		}
	}

	@Override
	public ArrayList<Card> getCards(int playerId) throws SQLException {
		connect();
		try (PreparedStatement statement = connection.prepareStatement(GET_CARD)) {
			statement.setInt(1, playerId);
			ResultSet resultSet = statement.executeQuery();
			ArrayList<Card> cards = new ArrayList<>();
			while (resultSet.next()) {
				int id = resultSet.getInt("id");
				String number = resultSet.getString("number");
				String color = resultSet.getString("color");
				cards.add(new Card(id, number, color, playerId));
			}
			return cards;
		} finally {
			disconnect();
		}
	}

	@Override
	public Card getCard(int cardId) throws SQLException {
		connect();
		try (PreparedStatement statement = connection.prepareStatement(SAVE_CARD_COT)) {
			statement.setInt(1, cardId);
			ResultSet resultSet = statement.executeQuery();
			resultSet.next();
			int id = resultSet.getInt("id");
			String number = resultSet.getString("number");
			String color = resultSet.getString("color");
			int playerId = resultSet.getInt("id_player");
			return new Card(id, number, color, playerId);
		} finally {
			disconnect();
		}
	}

	@Override
	public void saveGame(Card card) throws SQLException {
		connect();
		try (PreparedStatement statement = connection.prepareStatement(SAVE_CARD_DD)) {
			statement.setInt(1, card.getPlayerId());
			statement.setString(2, card.getNumber());
			statement.setString(3, card.getColor());
			statement.executeUpdate();
		} finally {
			disconnect();
		}
	}

	@Override
	public void saveCard(Card card) throws SQLException {
		connect();
		try (PreparedStatement statement = connection.prepareStatement(SAVE_CARD)) {
			statement.setInt(1, card.getPlayerId());
			statement.setString(2, card.getNumber());
			statement.setString(3, card.getColor());
			statement.executeUpdate();
		} finally {
			disconnect();
		}
	}

	@Override
	public void deleteCard(Card card) throws SQLException {
		connect();
		try (PreparedStatement statement = connection.prepareStatement(DELETE_CARD)) {
			statement.setInt(1, card.getId());
			statement.executeUpdate();
		} finally {
			disconnect();
		}
	}

	@Override
	public void clearDeck(int playerId) throws SQLException {
		connect();
		try (PreparedStatement statement = connection.prepareStatement(CLEAR_DECK)) {
			statement.setInt(1, playerId);
			statement.executeUpdate();
		} finally {
			disconnect();
		}
	}

	@Override
	public void addVictories(int playerId) throws SQLException {
		connect();
		try (PreparedStatement statement = connection.prepareStatement(ADD_VICTORIES)) {
			statement.setInt(1, playerId);
			statement.executeUpdate();
		} finally {
			disconnect();
		}
	}

	@Override
	public void addGames(int playerId) throws SQLException {
		connect();
		try (PreparedStatement statement = connection.prepareStatement(ADD_GAMES)) {
			statement.setInt(1, playerId);
			statement.executeUpdate();
		} finally {
			disconnect();
		}
	}
}
