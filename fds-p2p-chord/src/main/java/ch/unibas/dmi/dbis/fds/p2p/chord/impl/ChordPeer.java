package ch.unibas.dmi.dbis.fds.p2p.chord.impl;

import ch.unibas.dmi.dbis.fds.p2p.chord.api.*;
import ch.unibas.dmi.dbis.fds.p2p.chord.api.ChordNetwork;
import ch.unibas.dmi.dbis.fds.p2p.chord.api.data.Identifier;
import ch.unibas.dmi.dbis.fds.p2p.chord.api.data.IdentifierCircle;
import ch.unibas.dmi.dbis.fds.p2p.chord.api.data.IdentifierCircularInterval;
import ch.unibas.dmi.dbis.fds.p2p.chord.api.math.CircularInterval;
import ch.unibas.dmi.dbis.fds.p2p.chord.api.math.HashFunction;

import java.util.Optional;
import java.util.Random;

import static ch.unibas.dmi.dbis.fds.p2p.chord.api.data.IdentifierCircularInterval.createOpen;

/**
 * TODO: write JavaDoc
 *
 * @author loris.sauter
 */
public class ChordPeer extends AbstractChordPeer {
  /**
   *
   * @param identifier
   * @param network
   */
  protected ChordPeer(Identifier identifier, ChordNetwork network) {
    super(identifier, network);
  }

  /**
   * Asks this {@link ChordNode} to find {@code id}'s successor {@link ChordNode}.
   *
   * Defined in [1], Figure 4
   *
   * @param caller The calling {@link ChordNode}. Used for simulation - not part of the actual chord definition.
   * @param id The {@link Identifier} for which to lookup the successor. Does not need to be the ID of an actual {@link ChordNode}!
   * @return The successor of the node {@code id} from this {@link ChordNode}'s point of view
   */
  @Override
  public ChordNode findSuccessor(ChordNode caller, Identifier id) {
    ChordNode n_prime = findPredecessor(this, id);
        return n_prime.successor();

  }

  /**
   * Asks this {@link ChordNode} to find {@code id}'s predecessor {@link ChordNode}
   *
   * Defined in [1], Figure 4
   *
   * @param caller The calling {@link ChordNode}. Used for simulation - not part of the actual chord definition.
   * @param id The {@link Identifier} for which to lookup the predecessor. Does not need to be the ID of an actual {@link ChordNode}!
   * @return The predecessor of or the node {@code of} from this {@link ChordNode}'s point of view
   */
  @Override
  public ChordNode findPredecessor(ChordNode caller, Identifier id) {

    /* TODO: Implementation required. */
    ChordNode n_prime = this;

    while (!IdentifierCircularInterval.createLeftOpen(
            n_prime.getIdentifier(),
            n_prime.successor().getIdentifier()).contains(id)) {
      n_prime = n_prime.closestPrecedingFinger(this, id);
    }
    return n_prime;
  }

  /**
   * Return the closest finger preceding the  {@code id}
   *
   * Defined in [1], Figure 4
   *
   * @param caller The calling {@link ChordNode}. Used for simulation - not part of the actual chord definition.
   * @param id The {@link Identifier} for which the closest preceding finger is looked up.
   * @return The closest preceding finger of the node {@code of} from this node's point of view
   */
  @Override
  public ChordNode closestPrecedingFinger(ChordNode caller, Identifier id) {

    for (int i = getNetwork().getNbits(); i>=1; i--){
      if (finger().node(i).isEmpty()) {
        continue;
      }
      ChordNode fingerNode = finger().node(i).get();
      var interval = IdentifierCircularInterval.createOpen(
              getIdentifier(),
              id);
      if (interval.contains(fingerNode.getIdentifier())){
        return fingerNode;
      }
    }
    // if no closer finger could be found, return self.
    return this;

  }

  /**
   * Called on this {@link ChordNode} if it wishes to join the {@link ChordNetwork}. {@code nprime} references another {@link ChordNode}
   * that is already member of the {@link ChordNetwork}.
   *
   * Required for static {@link ChordNetwork} mode. Since no stabilization takes place in this mode, the joining node must make all
   * the necessary setup.
   *
   * Defined in [1], Figure 6
   *
   * @param nprime Arbitrary {@link ChordNode} that is part of the {@link ChordNetwork} this {@link ChordNode} wishes to join.
   */
  @Override
  public void joinAndUpdate(ChordNode nprime) {
    if (nprime != null) {
      initFingerTable(nprime);
      updateOthers();
      /* TODO: Move keys. */
      TransferKeysFromSuccessorToCurrent();
    } else {
      for (int i = 1; i <= getNetwork().getNbits(); i++) {
        this.fingerTable.setNode(i, this);
      }
      this.setPredecessor(this);
    }
  }

  public void TransferKeysFromSuccessorToCurrent(){
    ChordNode successor = successor();
    HashFunction hashFunction = getNetwork().getHashFunction();
    IdentifierCircularInterval interval = IdentifierCircularInterval.createLeftOpen(
            predecessor().getIdentifier(), getIdentifier()
    );
    for (String key : successor.keys()){
      var keyHash = hashFunction.hash(key);
      var keyIdentifier = getNetwork().getIdentifierCircle().getIdentifierAt(keyHash);
      if (!interval.contains(keyIdentifier)){
        continue;
      }
      //This gets value from the succesor.
      Optional<String> value = successor.lookup(this, key);
      if (value.isEmpty()){
        //if this is the case it means there was no value stored for the key therefor we just delete it.
        successor.delete(this,key);
        continue;
      }
      //here we store our <key:value> locally.
      store(this, key, value.get());

      //The key from the successor is no longer needed therefor we delete it.
      successor.delete(this, key);
    }
  }

  /**
   * Called on this {@link ChordNode} if it wishes to join the {@link ChordNetwork}. {@code nprime} references
   * another {@link ChordNode} that is already member of the {@link ChordNetwork}.
   *
   * Required for dynamic {@link ChordNetwork} mode. Since in that mode {@link ChordNode}s stabilize the network
   * periodically, this method simply sets its successor and waits for stabilization to do the rest.
   *
   * Defined in [1], Figure 7
   *
   * @param nprime Arbitrary {@link ChordNode} that is part of the {@link ChordNetwork} this {@link ChordNode} wishes to join.
   */
  @Override
  public void joinOnly(ChordNode nprime) {
    setPredecessor(null);
    if (nprime == null) {
      this.fingerTable.setNode(1, this);
    } else {
      this.fingerTable.setNode(1, nprime.findSuccessor(this,this));
    }
  }

  /**
   * Initializes this {@link ChordNode}'s {@link FingerTable} based on information derived from {@code nprime}.
   *
   * Defined in [1], Figure 6
   *
   * @param nprime Arbitrary {@link ChordNode} that is part of the network.
   */
  private void initFingerTable(ChordNode nprime) {
    var interval = getFingerTable().interval(1);
    fingerTable.setNode(1, nprime.findSuccessor(this, interval.getLeftBound()));
    setPredecessor(successor().predecessor());
    successor().setPredecessor(this);

    for (int i = 1; i <= getNetwork().getNbits() - 1; i++){
      if (finger().node(i).isEmpty()){
        continue;
      }
      ChordNode fingerNode = finger().node(i).get();
      interval = IdentifierCircularInterval.createRightOpen(getIdentifier(),fingerNode.getIdentifier());
      if (interval.contains(finger().interval(i+1).getLeftBound())){
        fingerTable.setNode(i+1, nprime.findSuccessor(this, finger().interval(i+1).getLeftBound()));
      }
    }

  }

  /**
   * Updates all {@link ChordNode} whose {@link FingerTable} should refer to this {@link ChordNode}.
   *
   * Defined in [1], Figure 6
   */
  private void updateOthers() {
    for (int i = 1; i <= getNetwork().getNbits(); i++) {
      // Error: findPredecessor always returns the last node for the given id,
      //        but sometimes we need to update the node at the given id,
      //        so we add +1.
      int id = getIdentifier().getIndex() + 1 - (int) Math.pow(2, i - 1);
      Identifier identifier = getNetwork().getIdentifierCircle().getIdentifierAt(id);

      ChordNode p = findPredecessor(
              this,
              identifier);
      p.updateFingerTable(this, i);
    }
    /* TODO: Implementation required. */

  }

  /**
   * If node {@code s} is the i-th finger of this node, update this node's finger table with {@code s}
   *
   * Defined in [1], Figure 6
   *
   * @param s The should-be i-th finger of this node
   * @param i The index of {@code s} in this node's finger table
   */
  @Override
  public void updateFingerTable(ChordNode s, int i) {
    finger().node(i).ifPresent(node -> {
      // Error: In the paper, the interval is specified as RightOpen, when it
      //        should actually be LeftOpen.
      var interval = IdentifierCircularInterval.createLeftOpen(
              getIdentifier(),
              node.getIdentifier()
      );

      if (interval.contains(s.getIdentifier())) {
        fingerTable.setNode(i, s);
        ChordNode p = predecessor();
        p.updateFingerTable(s, i);
      }
      /* TODO: Implementation required. */
    });
  }

  /**
   * Called by {@code nprime} if it thinks it might be this {@link ChordNode}'s predecessor. Updates predecessor
   * pointers accordingly, if required.
   *
   * Defined in [1], Figure 7
   *
   * @param nprime The alleged predecessor of this {@link ChordNode}
   */
  @Override
  public void notify(ChordNode nprime) {
    if (this.status() == NodeStatus.OFFLINE || this.status() == NodeStatus.JOINING) return;

    ChordNode p = predecessor();
    if (p == null) {
      setPredecessor(nprime);
      return;
    }

    var interval = IdentifierCircularInterval.createOpen(
            p.getIdentifier(),
            getIdentifier());
    if (interval.contains(nprime.getIdentifier())) {
      setPredecessor(nprime);
    }
    /* TODO: Implementation required. Hint: Null check on predecessor! */

  }

  /**
   * Called periodically in order to refresh entries in this {@link ChordNode}'s {@link FingerTable}.
   *
   * Defined in [1], Figure 7
   */
  @Override
  public void fixFingers() {
    if (this.status() == NodeStatus.OFFLINE || this.status() == NodeStatus.JOINING) return;
    int i = (int) (Math.random() * (finger().size() - 1) + 2);

    var id = getNetwork().getIdentifierCircle().getIdentifierAt(finger().start(i));

    fingerTable.setNode(
            i,
            findSuccessor(this, id));

    /* TODO: Implementation required */

  }

  /**
   * Called periodically in order to verify this node's immediate successor and inform it about this
   * {@link ChordNode}'s presence,
   *
   * Defined in [1], Figure 7
   */
  @Override
  public void stabilize() {
    if (this.status() == NodeStatus.OFFLINE || this.status() == NodeStatus.JOINING) return;

    ChordNode x = successor().predecessor();
    if (x != null){
      var interval = IdentifierCircularInterval.createOpen(
              getIdentifier(),
              successor().getIdentifier());

      if (interval.contains(x.getIdentifier())) {
        // Set x as successor
        fingerTable.setNode(1, x);
      }
    }
    successor().notify(this);
    /* TODO: Implementation required.Still need to test this*/

  }

  /**
   * Called periodically in order to check activity of this {@link ChordNode}'s predecessor.
   *
   * Not part of [1]. Required for dynamic network to handle node failure.
   */
  @Override
  public void checkPredecessor() {
    if (this.status() == NodeStatus.OFFLINE || this.status() == NodeStatus.JOINING) return;
    // check if predecessor is null and if so do nothing
    ChordNode predecessor = predecessor();
    if (predecessor == null) {
      return;
    }

    // do nothing when predecessor is still online
    if (predecessor.status() == NodeStatus.ONLINE) {
      return;
    }

    // when predecessor is offline, look for next preceding online node and make it new predecessor
    if (predecessor.status() == NodeStatus.OFFLINE) {
      this.setPredecessor(null);
    }
  }

  /**
   * Called periodically in order to check activity of this {@link ChordNode}'s successor.
   *
   * Not part of [1]. Required for dynamic network to handle node failure.
   */
  @Override
  public void checkSuccessor() {
    if (this.status() == NodeStatus.OFFLINE || this.status() == NodeStatus.JOINING) return;

    ChordNode successor = this.successor();

    // if successor is null do nothing
    if (successor == null) {
      return;
    }

    // if successor is online do nothing
    if (successor.status() == NodeStatus.ONLINE) {
      return;
    }

    // if successor is offline, look for next succeeding node that is online and make it new successor
    if (successor.status() == NodeStatus.OFFLINE) {
      this.fingerTable.setNode(1,this.findSuccessor(this, this));
    }

  }

  /**
   * Performs a lookup for where the data with the provided key should be stored.
   *
   * @return Node in which to store the data with the provided key.
   */
  @Override
  protected ChordNode lookupNodeForItem(String key) {
    if (keys().contains(key)) {
      return this;
    }
    HashFunction hashFunction = getNetwork().getHashFunction();
    var hashVal = hashFunction.hash(key);
    return findSuccessor(this, getNetwork().getIdentifierCircle().getIdentifierAt(hashVal));
  }

  @Override
  public String toString() {
    return String.format("ChordPeer{id=%d}", this.id().getIndex());
  }
}
